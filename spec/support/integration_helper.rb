# frozen_string_literal: true

require "securerandom"
require "open3"
require "timeout"

module IntegrationHelper
  def self.included(klass)
    klass.instance_eval do
      before(:all) do
        @@topic_handles ||= {}
      end

      after do
        incoming_messages.clear
        @rdkafka_consumer && @rdkafka_consumer.close
        @rdkafka_producer && @rdkafka_producer.close

        @rdkafka_admin && @rdkafka_admin.close
        wait_for_child_processes
        reset_signal_handlers
      end

      after(:all) do
        delete_test_topics
      end
    end
  end

  def rdkafka_consumer
    @rdkafka_consumer ||= Rdkafka::Config.new({
      "bootstrap.servers": kafka_brokers,
      "client.id":         Racecar.config.client_id,
      "group.id":          "racecar-tests",
      "auto.offset.reset": "beginning"
    }.merge(Racecar.config.rdkafka_consumer)).consumer
  end

  def rdkafka_admin
    @rdkafka_admin ||= Rdkafka::Config.new({
      "bootstrap.servers": kafka_brokers,
    }).admin
  end

  def rdkafka_producer
    @rdkafka_producer ||= Rdkafka::Config.new({
      "bootstrap.servers": kafka_brokers,
    }).producer
  end

  def drip_messages(count, topic, partitions, period)
    counter = 0
    count.times.each do |n|
      execute_in_time(period) do
        partitions.times.map do |pn|
          rdkafka_producer.produce(
            topic: topic,
            payload: "message #{count} #{pn} / #{counter}",
            partition: pn,
          )
        end.each(&:wait)
        counter += 1
      end
    end
    debug "Done dripping messages 💧💧💧 x #{counter}"
  end

  def execute_in_time(period, &block)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    block.call
    finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    elapsed = finish - start
    sleep_time = period - elapsed
    if sleep_time > 0
      sleep(sleep_time)
    else
      warn "Warning: Execution of #{block} took longer than #{period}s"
    end
  end

  def publish_messages!(topic, messages)
    messages.map do |m|
      rdkafka_producer.produce(
        topic: topic,
        key: m.fetch(:key, nil),
        payload: m.fetch(:payload),
        partition: m.fetch(:partition, nil),
      )
    end.each(&:wait)

    # $stderr.puts "Published messages to topic: #{topic}; messages: #{messages}"
  end

  def create_topic(topic:, partitions: 1, replication_factor: 1)
    $stderr.puts "Creating topic #{topic}"
    handle = rdkafka_admin.create_topic(topic, partitions, replication_factor)
    @@topic_handles[topic] = handle
    handle.wait
  end

  def wait_for_messages(topic:, expected_message_count:)
    rdkafka_consumer.subscribe(topic)

    attempts = 0

    while incoming_messages.count < expected_message_count && attempts < 20
      $stderr.puts "Waiting for messages..."
      attempts += 1

      while (message = rdkafka_consumer.poll(1000))
        $stderr.puts "Received message #{message}"
        incoming_messages << message
      end
    end
  end

  def generate_input_topic_name
    "#{input_topic_prefix}-#{SecureRandom.hex(8)}"
  end

  def generate_output_topic_name
    "#{output_topic_prefix}-#{SecureRandom.hex(8)}"
  end

  def delete_test_topics
    @@topic_handles.keys.map { |topic_name|
      $stdout.puts "Deleting topic #{topic_name}"
      rdkafka_admin.delete_topic(topic_name)
    }.each(&:wait)
  end

  def incoming_messages
    @incoming_messages ||= []
  end

  private

  def kafka_brokers
    Racecar.config.brokers.join(",")
  end

  def input_topic_prefix
    "input-test-topic"
  end

  def output_topic_prefix
    "output-test-topic"
  end

  def in_background(cleanup_callback:, &block)
    Thread.new do
      begin
        block.call
      ensure
        cleanup_callback.call
      end
    end
  end

  def reset_signal_handlers
    ["INT", "TERM", "QUIT"].each do |signal|
      Signal.trap(signal, "DEFAULT")
    end
  end

  def wait_for_child_processes
    Timeout.timeout(5) do
      Process.waitall
    end
  rescue Timeout::Error
    warn "Timed out waiting for child processes to exit, may have left zombie processes."
  end
end
