# frozen_string_literal: true

require "securerandom"
require "racecar/cli"
require "racecar/ctl"

require "active_support/core_ext/hash"
require "ap"

class NoSubsConsumer < Racecar::Consumer
  def process(message); end
end

class NoProcessConsumer < Racecar::Consumer
  subscribes_to "some-topic"
end

RSpec.describe "running a Racecar consumer", type: :integration do

  context "cooperative-sticky assignment" do
    before do
      create_topic(topic: input_topic, partitions: topic_partitions)
      create_topic(topic: output_topic, partitions: topic_partitions)
      # FileUtils.mkdir_p(File.dirname(consumer_class_code_file))
      # File.write(consumer_class_code_file, consumer_class_code)
    end

    after do
      @drip_thread && @drip_thread.terminate
      # FileUtils.rm_f(consumer_class_code_file)
    end

    let(:message_count) { topic_partitions * 3 }
    let(:topic_partitions) { 4 }
    let(:input_topic) { generate_input_topic_name }
    let(:output_topic) { generate_output_topic_name }
    let(:message_iterations) { 200 }
    let(:expected_message_count) { message_iterations * topic_partitions }
    let(:freq) { 2 }
    let(:consumer_pids) { [] }
    let(:spawned_threads) { [] }

    # let(:consumer_class_code_file) { "tmp/test_consumer.rb" }

    MessageRecord = Struct.new(:consumer_pid, :created_at, :occurred_at, :partition, :offset, :payload, keyword_init: true) do
      def initialize(**args)
        super
        self.created_at = Time.parse(created_at)
        self.occurred_at = Time.parse(occurred_at)
      end

      def <=>(other)
        [other.partition, other.offset] <=> [partition, offset]
      end

      def latency
        created_at - occurred_at
      end
    end

    fit do
      Signal.trap("INT") do
        kill_processes
        spawned_threads.select(&:alive?).each(&:terminate)
      end

      # Signal.trap("CHLD") do
      #   puts "A child! NO!"
      #   consumer_pids.each do |pid|
      #     result = Process.wait2(pid, Process::WNOHANG) rescue $!
      #     puts "chcking #{pid} #{result}"
      #   end
      # end

      start_consumer
      start_consumer

      spawned_threads << Thread.new do
        sleep 3
        drip_messages(message_iterations, input_topic, topic_partitions, 1.0/freq)
      end

      status_thread = Thread.new do
        # 10.times do
        #   sleep 5
        #   debug ""
        #   debug " 🗄️🗄️🗄️🗄️🗄️🗄️🗄️🗄️ Files status"
        #   debug show_how_those_files_are_doing
        # end
      end

      puts "Waiting for first consumer consume a bit"
      wait_for_consumer_to_process_messages(index: 1, count: 30)

      puts "Terminating first consumer"
      term_consumer(1)

      puts "Waiting to simulate boot time"
      wait_as_if_the_new_consumer_was_booting(5)
      start_consumer

      puts "Waiting for all messages"
      wait_for_message_processing(expected_message_count)

      empty_by_partition = topic_partitions.times.map { |pn| [pn, []] }.to_h

      by_partition = message_records.group_by(&:partition).reverse_merge(empty_by_partition)
      uniq_by_partition = by_partition.transform_values { |ms| ms.uniq(&:offset) }

      missing_messages_by_partition = uniq_by_partition.transform_values { |ms|
        (0...message_iterations).reject { |offset| ms.detect { |m| m.offset == offset } }
      }

      duplicates_by_partition = by_partition.transform_values { |ms|
        ms.group_by(&:offset)
          .select { |_offset, ms| ms.size > 1 }
          .values
          .flatten
      }

      empty_by_consumer = consumer_pids.map { |pid| [pid, []] }.to_h
      duplicates_by_consumer = duplicates_by_partition
        .values
        .flatten
        .group_by(&:consumer_pid)
        .reverse_merge(empty_by_consumer)

      offsets_by_partition = by_partition.transform_values { |ms| ms.map(&:offset).sort }
      stats_by_partition = by_partition.map do |partition, ms|
        furthest_apart = ms.each_cons(2).max_by { |a,b| b.created_at - a.created_at }
        most_latent = ms.max_by(&:latency)

        stats = {
          most_latent: most_latent,
          max_latency: most_latent.latency,
          furthest_apart: furthest_apart,
          max_interval: furthest_apart.map(&:created_at).reverse.reduce(:-),
        }
        [partition, stats]
      end.to_h

      max_latency_overall = stats_by_partition.max_by { |_partition, stats| stats[:max_latency] }.last
      max_interval_overall = stats_by_partition.max_by { |_partition, stats| stats[:max_interval] }.last

      aggregate_failures do
        expect(files).to all be_readable

        expect(message_records.count).to eq(expected_message_count)

        topic_partitions.times do |pn|
          expect(offsets_by_partition[pn]).to be_consective
        end

        # expect(missing_messages_by_partition).to eq(empty_by_partition)
        expect(duplicates_by_partition).to eq(empty_by_partition)
        expect(duplicates_by_consumer).to eq(empty_by_consumer)

        expect(max_interval_overall[:max_interval]).to be < 8.0
        expect(max_latency_overall[:max_latency]).to be < 2.0
      end
    end

    def wait_as_if_the_new_consumer_was_booting(seconds)
      sleep(seconds)
    end

    def wait_for_consumer_to_process_messages(index:, count:, slack: 2)
      file = files.fetch(index)
      max_wait = count + slack

      debug "😴⏰ Waiting #{max_wait}s for consumer #{index} to process #{count} messages"

      Timeout.timeout(max_wait) do
        until file.readable? && file.readlines.size > count
          sleep 1
        end
      end
    end

    def start_consumer
      consumer_pids << fork do
        debug "Forked new consumer #{consumer_pids.size} pid=#{Process.pid}"
        ENV["TOPIC"] = input_topic
        ENV["CONSUMER_N"] = consumer_pids.size.to_s
        exec("bundle exec racecar TestConsumer -r spec/support/test_consumer.rb")
      end
    end

    def term_consumer(index)
      pid = consumer_pids.fetch(index)
      debug "Sending TERM to consumer #{index} pid=#{pid}"
      Process.kill("TERM", pid)
    end

    def wait_for_message_processing(expected_message_count)
      max_wait = (message_iterations / freq) * 1.5
      puts "waiting for messages, will wait #{max_wait}"

      Timeout.timeout(max_wait) do
        until readlines.size >= expected_message_count
          sleep(2/freq)
        end
      end
    rescue Timeout::Error => e
      message = "#{e.message}. Expected #{expected_message_count} messages, got #{readlines.size}"
      debug message
      # raise Timeout::Error.new(message).tap { |ne| ne.set_backtrace(e.backtrace) }
    end

    def show_how_those_files_are_doing
      time = Time.now.strftime("%H:%M:%S")
      file_names.map do |f|
        messages_count = File.exist?(f) && File.readlines(f).size
        "#{time} #{f} count: #{messages_count}"
      end
    end

    def message_records
      @message_records ||= readlines
        .map { |dz| JSON.parse(dz).transform_keys(&:to_sym) }
        .map { |h| MessageRecord.new(**h) }
        .sort_by(&:created_at)
    end

    def kill_processes
      consumer_pids.each do |pid|
        Process.kill("TERM", pid) rescue nil
      end
    end

    def readlines
      file_names
        .select { |fn| File.exist?(fn) }
        .flat_map { |fn| File.readlines(fn) }
    end

    def files
      file_names.map { |f| Pathname.new(f) }
    end

    def file_names
      consumer_pids.map { |pid| "tmp/consumer-#{pid}" }
    end

    RSpec::Matchers.define :be_consective do
      match do |sequence|
        sequence.each_cons(2).all? { |a,b| a.succ == b }
      end
    end
    RSpec::Matchers.define :be_empty_for_all_partitions do
      match do |actual_by_partition|
        actual_by_partition == empty_by_partition
      end

      def empty_by_partition
        topic_partitions.times.map { |n| [n, []] }.to_h
      end
    end

    RSpec::Matchers.define :be_empty_for_all_consumers do
      match do |actual_by_partition|
        actual_by_partition == empty_by_consumer
      end

      def empty_by_partition
        consumer_pids.map { |pid| [pid, []] }.to_h
      end
    end

    after  do
      kill_processes
      Process.waitall

      file_names.each_with_index do |f,i|
        FileUtils.cp(f, "tmp/consumer-#{i}.jsons" )
      end
    end

    it "stats" do
      ENV["TOPIC"] = input_topic
      ENV["CONSUMER_N"] = "0"

      require "support/test_consumer"
      cli = Racecar::Cli.new(["TestConsumer"])

      cli_thread = Thread.new {
        drip_messages(10, input_topic, topic_partitions, 0.2)
      }

      cli.run

      stats = $consumer_stats

      wait_for_message_processing(5 * topic_partitions)
    end
  end

  context "when an error occurs trying to start the runner" do
    context "when there are no subscriptions, and no parallelism" do
      before { NoSubsConsumer.parallel_workers = nil }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoSubsConsumer"]).run
        end.to raise_error(ArgumentError)
      end
    end

    context "when there are no subscriptions, and parallelism" do
      before { NoSubsConsumer.parallel_workers = 3 }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoSubsConsumer"]).run
        end.to raise_error(ArgumentError)
      end
    end

    context "when there is no process method, and no parallelism" do
      before { NoProcessConsumer.parallel_workers = nil }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoProcessConsumer"]).run
        end.to raise_error(NotImplementedError)
      end
    end

    context "when there is no process method, and parallelism" do
      before { NoSubsConsumer.parallel_workers = 3 }

      it "raises an exception" do
        expect do
          Racecar::Cli.new(["NoProcessConsumer"]).run
        end.to raise_error(NotImplementedError)
      end
    end
  end

  context "when the runner starts successfully" do
    let(:input_topic) { generate_input_topic_name }
    let(:output_topic) { generate_output_topic_name }

    before do
      create_topic(topic: input_topic, partitions: topic_partitions)
      create_topic(topic: output_topic, partitions: topic_partitions)

      consumer_class.subscribes_to(input_topic)
      consumer_class.output_topic = output_topic
      consumer_class.parallel_workers = parallelism

      publish_messages!(input_topic, input_messages)
    end

    context "for a single threaded consumer" do
      let(:input_messages) { [{ payload: "hello", key: "greetings", partition: nil }] }
      let(:topic_partitions) { 1 }
      let(:parallelism) { nil }

      it "can consume and publish a message" do
        cli = Racecar::Cli.new([consumer_class.name.to_s])
        in_background(cleanup_callback: -> { cli.stop }) do
          wait_for_messages(topic: output_topic, expected_message_count: input_messages.count)
        end

        cli.run

        message = incoming_messages.first

        expect(message).not_to be_nil
        expect(message.topic).to eq output_topic
        expect(message.payload).to eq "hello"
        expect(message.key).to eq "greetings"
      end
    end

    context "when running parallel workers" do
      let(:input_messages) do
        [
          { payload: "message-0", partition: 0, key: "a" },
          { payload: "message-1", partition: 1, key: "a" },
          { payload: "message-2", partition: 2, key: "a" },
          { payload: "message-3", partition: 3, key: "a" },
          { payload: "message-4", partition: 4, key: "a" },
          { payload: "message-5", partition: 5, key: "a" }
        ]
      end

      context "when partitions exceed parallelism" do
        let(:topic_partitions) { 6 }
        let(:parallelism) { 3 }

        it "assigns partitions to all parallel workers" do
          cli = Racecar::Cli.new([consumer_class.name.to_s])
          in_background(cleanup_callback: -> { cli.stop }) do
            wait_for_messages(topic: output_topic, expected_message_count: input_messages.count)
          end

          cli.run

          message_count_by_worker = incoming_messages.group_by { |m| m.headers.fetch(:processed_by_pid) }.transform_values(&:count)

          expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
          expect(incoming_messages.map(&:payload))
            .to match_array(input_messages.map { |m| m[:payload] })
          expect(message_count_by_worker.values).to eq([2,2,2])
        end
      end

      context "when the parallelism exceeds the number of partitions" do
        let(:topic_partitions) { 3 }
        let(:parallelism) { 5 }
        let(:input_messages) do
          [
            { payload: "message-0", partition: 0, key: "a" },
            { payload: "message-1", partition: 0, key: "a" },
            { payload: "message-2", partition: 1, key: "a" },
            { payload: "message-3", partition: 1, key: "a" },
            { payload: "message-4", partition: 2, key: "a" },
            { payload: "message-5", partition: 2, key: "a" }
          ]
        end

        it "assigns all the consumers that it can, up to the total number of partitions" do
          cli = Racecar::Cli.new([consumer_class.name.to_s])
          in_background(cleanup_callback: -> { cli.stop }) do
            wait_for_messages(topic: output_topic, expected_message_count: input_messages.count)
          end

          cli.run

          message_count_by_worker = incoming_messages.group_by { |m| m.headers.fetch(:processed_by_pid) }.transform_values(&:count)

          expect(incoming_messages.count).to eq(6)
          expect(incoming_messages.map(&:topic).uniq).to eq([output_topic])
          expect(incoming_messages.map(&:payload))
            .to match_array(input_messages.map { |m| m[:payload] })
          expect(message_count_by_worker.values).to eq([2,2,2])
        end
      end
    end
  end

  let(:consumer_class) do
    TestConsumer = echo_consumer_class
  end

  after do
    Object.send(:remove_const, :TestConsumer) if defined?(TestConsumer)
  end

  def echo_consumer_class
    Class.new(Racecar::Consumer) do
      class << self
        attr_accessor :output_topic
      end
      self.group_id = "test_consumer_#{SecureRandom.hex(4)}"

      def process(message)
        produce(message.value, key: message.key, topic: self.class.output_topic, headers: headers)
        deliver!
      end

      private

      def headers
        {
          processed_by_pid: Process.pid,
        }
      end
    end
  end
end
