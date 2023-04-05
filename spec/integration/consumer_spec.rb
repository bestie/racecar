# frozen_string_literal: true

require "securerandom"
require "racecar/cli"
require "racecar/ctl"

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
      Racecar.configure do |config|
        config.consumer = [
          "auto.commit.interval.ms=1000",
          'statistics.interval.ms=1000',
          'session.timeout.ms=6000',
          'heartbeat.interval.ms=1500',
          'partition.assignment.strategy=cooperative-sticky'
        ]
      end

      create_topic(topic: input_topic, partitions: topic_partitions)
      create_topic(topic: output_topic, partitions: topic_partitions)
      FileUtils.mkdir_p(File.dirname(consumer_class_code_file))
      File.write(consumer_class_code_file, consumer_class_code)
    end

    after do
      @drip_thread.terminate
      FileUtils.rm_f(consumer_class_code_file)
    end

    # let(:input_messages) { message_count.times.map { |n|
    #   { payload: "message-#{n}", partition: n % topic_partitions }
    # } }

    let(:message_count) { topic_partitions * 3 }
    let(:topic_partitions) { 4 }
    let(:input_topic) { generate_input_topic_name }
    let(:output_topic) { generate_output_topic_name }
    let(:message_iterations) { 100 }
    let(:expected_message_count) { message_iterations * topic_partitions }
    let(:freq) { 2 }
    let(:consumer_pids) { [] }

    let(:consumer_class_code_file) { "tmp/test_consumer.rb" }
    let(:consumer_class_code) { <<~RUBY }
      require "time"

      class TestConsumer < Racecar::Consumer
        Racecar.configure do |config|
          config.consumer = [
            'statistics.interval.ms=1000',
            'session.timeout.ms=6000',
            'heartbeat.interval.ms=1500',
            'partition.assignment.strategy=cooperative-sticky'
          ]
        end
        subscribes_to "#{input_topic}"
        self.group_id = "stickies"

        def configure(*args, &block)
          @filename = "tmp/consumer-\#{Process.pid}"
          super
        end

        def process(message)
          # puts "\#{Process.pid} message \#{message.partition}/\#{message.offset}"

          message_data = JSON.dump(
            consumer_pid: Process.pid,
            created_at: Time.now.iso8601(6),
            occurred_at: message.create_time.iso8601(6),
            partition: message.partition,
            offset: message.offset,
          )

          File.open(@filename, "a") do |f|
            f.puts(message_data)
          end
        end

        def statistics_callback(stats)
          # TODO: use this to get the assigned partitions and the committed offsets
          #       would be nice to reveal uncommitted offsets on revokation
          p stats
        end
      end
    RUBY

    MessageRecord = Struct.new(:consumer_pid, :created_at, :occurred_at, :partition, :offset, keyword_init: true) do
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

    it do
      Signal.trap("INT") do
        kill_processes
      end

      Signal.trap("CHLD") do
        puts "A child! NO!"
        consumer_pids.each do |pid|
          result = Process.wait2(pid, Process::WNOHANG) rescue $!
          puts "chcking #{pid} #{result}"
        end
      end

      start_consumer
      start_consumer

      @drip_thread = Thread.new do
        drip_messages(message_iterations, input_topic, topic_partitions, 1.0/freq)
      end
      status_thread = Thread.new do
        5.times do
          sleep 5
          debug ""
          debug " 🗄️🗄️🗄️🗄️🗄️🗄️🗄️🗄️ Files status"
          debug show_how_those_files_are_doing
        end
      end

      wait_for_consumer_to_process_messages(index: 1, count: 2)

      term_consumer(1)

      wait_for_new_consumer_boot
      start_consumer

      wait_for_message_processing

      uniq_messages = message_records.uniq { |m| [m.partition, m.offset] }

      by_partition = message_records.group_by(&:partition)
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

      ap stats_by_partition

      max_latency_overall = stats_by_partition.max_by { |_partition, stats| stats[:max_latency] }.last
      max_interval_overall = stats_by_partition.max_by { |_partition, stats| stats[:max_interval] }.last

      aggregate_failures do
        expect(files).to all be_readable
        expect(message_records.count).to eq(expected_message_count)
        expect(uniq_messages.count).to eq(expected_message_count)
        expect(offsets_by_partition[0]).to eq((0..99).to_a)
        expect(max_interval_overall[:max_interval]).to be < 8.0
        expect(max_latency_overall[:max_latency]).to be < 2.0
      end
    end

    def wait_for_new_consumer_boot
      sleep 10
    end

    def wait_for_consumer_to_process_messages(index:, count:, slack: 2)
      file = files.fetch(index)
      max_wait = count + slack

      debug "😴⏰ Waiting #{max_wait}s for consumer #{index} to process #{count} messages"

      Timeout.timeout(max_wait) do
        until file.readable? && file.size > 0
          sleep 1
        end
      end
    end

    def start_consumer
      consumer_pids << fork do
        debug "Forked new consumer #{consumer_pids.size} pid=#{Process.pid}"
        exec("bundle exec racecar TestConsumer -r tmp/test_consumer.rb")
      end
    end

    def term_consumer(index)
      pid = consumer_pids.fetch(index)
      debug "Sending TERM to consumer #{index} pid=#{pid}"
      Process.kill("TERM", pid)
    end

    def wait_for_message_processing
      Timeout.timeout(message_iterations / freq) do
        until read_files.size >= expected_message_count
          sleep 0.5
        end
      end
    rescue Timeout::Error => e
      message = "#{e.message}. Expected #{expected_message_count} messages, got #{read_files.size}"
      require "pry"; binding.pry # DEBUG @bestie
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
      @message_records ||= read_files
        .map { |dz| JSON.parse(dz).transform_keys(&:to_sym) }
        .map { |h| MessageRecord.new(**h) }
        .sort_by(&:created_at)
    end

    def kill_processes
      consumer_pids.each do |pid|
        Process.kill("TERM", pid) rescue nil
      end
    end

    def read_files
      file_names.flat_map { |fn| File.exist?(fn) && File.readlines(fn) }
    end

    def files
      file_names.map { |f| Pathname.new(f) }
    end

    def file_names
      consumer_pids.map { |pid| "tmp/consumer-#{pid}" }
    end

    after  do
      kill_processes
      Process.waitall

      `rm tmp/consumer-*`
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

  def debug(thing, io = $stdout)
    return unless ENV["DEBUG"]
    # pink on yellow 🤩
    if thing.is_a?(Enumerable)
      thing.each { |tt| debug(tt) }
    else
      io.puts "\e[1;95;103m#{thing}\e[0m"
    end
  end
end
