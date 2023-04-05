# frozen_string_literal: true

require "securerandom"
require "racecar/cli"
require "racecar/ctl"

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
          'statistics.interval.ms=1000',
          'session.timeout.ms=6000',
          'heartbeat.interval.ms=1500',
          'partition.assignment.strategy=cooperative-sticky'
        ]
      end

      create_topic(topic: input_topic, partitions: topic_partitions)
      create_topic(topic: output_topic, partitions: topic_partitions)
      File.write("tmp/test_consumer.rb", consumer_class_code)
    end

    # let(:input_messages) { message_count.times.map { |n|
    #   { payload: "message-#{n}", partition: n % topic_partitions }
    # } }

    let(:message_count) { topic_partitions * 3 }
    let(:topic_partitions) { 4 }
    let(:input_topic) { generate_input_topic_name }
    let(:output_topic) { generate_output_topic_name }
    let(:test_run_time) { 30 }

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

        def configure(*args, &block)
          @filename = "tmp/consumer-\#{Process.pid}"
          super
        end

        def process(message)
          puts "\#{Process.pid} message \#{message.partition}/\#{message.offset}"

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
          # puts "got stats"
        end
      end
    RUBY

    MessageRecord = Struct.new(:consumer_pid, :created_at, :occurred_at, :partition, :offset, keyword_init: true) do
      def initialize(**args)
        super
        self.created_at = Time.parse(created_at)
        self.occurred_at = Time.parse(occurred_at)
      end
    end

    it do
      @pids = 2.times.map do
        fork do
          exec("bundle exec racecar TestConsumer -r tmp/test_consumer.rb")
        end
      end

      Signal.trap("INT") do
        kill_processes
      end

      drip_thread = Thread.new do
        drip_messages(test_run_time, input_topic, topic_partitions, 1.0)
      end

      sleep 5

      Process.kill("TERM", @pids.last)

      @pids << fork do
        exec("bundle exec racecar TestConsumer -r tmp/test_consumer.rb")
      end

      Timeout.timeout(30) do
        sleep 0.5 until read_files.size == test_run_time*topic_partitions
      end

      by_cosumer = message_records.group_by(&:consumer_pid)

      pp by_cosumer.transform_values(&:count)
    end

    def message_records
      @message_records ||= read_files
        .map { |dz| JSON.parse(dz).transform_keys(&:to_sym) }
        .map { |h| MessageRecord.new(**h) }
        .sort_by(&:created_at)
    end

    def kill_processes
      @pids.each do |pid|
        Process.kill("TERM", pid) rescue nil
      end
    end

    def read_files
      file_names.flat_map { |fn| File.readlines(fn) }
    end

    def file_names
      @pids.map { |pid| "tmp/consumer-#{pid}" }
    end

    after  do
      kill_processes
      Process.waitall

      @pids.each { |pid| File.unlink("tmp/consumer-#{pid}") }
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
