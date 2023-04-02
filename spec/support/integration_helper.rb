# frozen_string_literal: true

require "securerandom"
require "open3"

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

  def publish_messages!(topic, messages)
    messages.map do |m|
      rdkafka_producer.produce(
        topic: topic,
        key: m.fetch(:key, nil),
        payload: m.fetch(:payload),
        partition: m.fetch(:partition, nil),
      )
    end.each(&:wait)

    $stderr.puts "Published messages to topic: #{topic}; messages: #{messages}"
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

  def wait_for_assignments(group_id:, topic:, expected_members_count:)
    rebalance_attempt = 1

    until members_count(group_id, topic) == expected_members_count
      $stderr.puts "Waiting for rebalance..."
      sleep 2 * rebalance_attempt
      raise "Did not rebalance in time" if rebalance_attempt > 5
      rebalance_attempt += 1
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

  def members_count(group_id, topic)
    consumer_group_partitions_and_member_ids(group_id, topic).uniq { |data| data.fetch(:member_id) }.count
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

  def consumer_group_partitions_and_member_ids(group_id, topic)
    message, process = run_kafka_command(
      "kafka-consumer-groups --bootstrap-server #{kafka_brokers} --describe --group #{group_id}"
    )
    unless process.exitstatus.zero?
      raise "Kafka consumer group inspection exited with status #{process.exitstatus}, message: #{message}"
    end

    return [] if message.match?(/consumer.+(is rebalancing|does not exist|has no active members)/i)

    message
      .scan(/^#{group_id}\ +#{topic}\ +(?<partition>\d+)\ +\S+\ +\S+\ +\S+\ +(?<member_id>\S+)\ /)
      .map { |partition, member_id| { partition: partition, member_id: member_id } }
      .tap do |partitions|
        raise("Unexpected command output, please review regexp matcher:\n\n #{message}") if partitions.empty?
      end
  end

  def run_kafka_command(command)
    maybe_sudo = "sudo " if ENV["DOCKER_SUDO"] == "true"

    Open3.capture2e(
      "#{maybe_sudo}docker exec -t $(#{maybe_sudo}docker ps | grep broker | awk '{print $1}') #{command}"
    )
  end
end
