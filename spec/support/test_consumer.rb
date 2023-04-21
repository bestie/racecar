# Used in its own fork
# Make sure you load and configure everything it needs

require "time"
require "timeout"
require "benchmark"
require "active_support/core_ext/hash"

Thread.abort_on_exception = true

module Racecar
  class ConsumerStats
    def initialize(stats_hash)
      @stats_hash = stats_hash
    end
  end
end

class TestConsumer < Racecar::Consumer
  prepend UsefulInfo
  # prepend StatsThings

  Racecar.configure do |config|
    config.offset_commit_interval = 1
    config.consumer = [
      'statistics.interval.ms=1000',
      'session.timeout.ms=6000',
      'heartbeat.interval.ms=1500',
      'partition.assignment.strategy=cooperative-sticky',
      # "debug=cgrp,topic,fetch",
      # "debug=all",
    ]
  end
  subscribes_to ENV.fetch("TOPIC"), start_from_beginning: true
  self.group_id = "stickies"

  def configure(*args, &block)
    @filename = "tmp/consumer-#{Process.pid}"
    @message_count_by_partition = Hash.new { |h,k| h[k] = 0 }

    @stats = []
    register_at_exit_hook
    @offsets_by_partition = Hash.new { |h,k| h[k] = MinMax.new }
    @starting_offset_by_partition = {}
    @assignments = nil
    @reporter_thread = Thread.new do
      loop do
        sleep 2
        if @assignments != current_assignments
          puts_yellow "New assignments: #{current_assignments}"
          @assignments = current_assignments
        end
      end
    end

    super
  end

  def process(message)
    # puts "#{Process.pid} message #{message.partition}/#{message.offset}"

    message_data = JSON.dump(
      consumer_pid: Process.pid,
      created_at: Time.now.iso8601(6),
      occurred_at: message.create_time.iso8601(6),
      partition: message.partition,
      offset: message.offset,
      payload: message.value,
    )

    File.open(@filename, "a") do |f|
      f.puts(message_data)
    end


    current_offset = @offsets_by_partition[message.partition].max

    committed_offset = committed_offsets&.dig(0, message.topic, message.partition)
    if committed_offset && message.offset < committed_offset
      puts_red "Replay on already committed offset p#{message.partition} #{committed_offset} => #{message.offset}  (processed up to #{current_offset}"
    end

    # if message.offset < current_offset
    #   puts_red "Got a rewind! p#{message.partition} #{current_offset} => #{message.offset}"
    # end

    @offsets_by_partition[message.partition] << message.offset
    @message_count_by_partition[message.partition] += 1
    # @starting_offset_by_partition[message.partition] ||= message.offset

  rescue Object => e
    debug "Failed to process message #{message.inspect}"
    puts e.full_message
    puts e.backtrace
  ensure
  end

  def puts(string)
    super "[#{Process.pid}] (#{ENV["CONSUMER_N"]}) #{string}"
  end

  def debug(string)
    super "[#{Process.pid}] (#{ENV["CONSUMER_N"]}) #{string}"
  end

  def puts_yellow(string)
    super "[#{Process.pid}] (#{ENV["CONSUMER_N"]}) #{string}"
  end

  def puts_red(string)
    super "[#{Process.pid}] (#{ENV["CONSUMER_N"]}) #{string}"
  end

  def register_at_exit_hook
    at_exit do
      total = @message_count_by_partition.values.sum
      puts_yellow "  processed messages (#{total}) #{@message_count_by_partition}\n" \
        "min/max offsets #{@offsets_by_partition}"
      puts_yellow "  committed offsets #{@committed_offsets}"
      # puts_yellow "  starting offsets #{@starting_offset_by_partition}"
    end
  end

  def teardown
    @reporter_thread.terminate if @reporter_thread.alive?
    @committed_offsets = committed_offsets
  end
end

BEGIN {
module UsefulInfo
  def member_ids
    rdk_consumers.map(&:member_id)
  end

  def committed_offsets
    extremely_generous_timeout = 20 # 5ms would probably be generous
    rdk_consumers.map { |rdkc|
      rdkc
        .committed(_list=nil, extremely_generous_timeout)
        .to_h
        .transform_values { |ps|
          ps.map { |part| [part.partition, part.offset] }.to_h
        }
    }
  rescue Rdkafka::RdkafkaError => e
    # puts "Timed out getting committed_offsets"
  end

  def current_assignments
    rdk_consumers.first.assignment.to_h
  end

  def all_partition_lists_by_topic
    merge_partition_lists_hashes(current_partition_list_topic_hashes)
  end

  def merge_partition_lists_hashes(hashes)
    hashes.reduce { |agg, partition_list_by_topic|
      agg.merge(partition_list_by_topic) { |topic, existing_list, new_list|
        existing_list + new_list
      }
    }
  end

  def current_partition_list_topic_hashes
    rdk_consumers.map { |rdkc| rdkc.assignment.to_h }
  end

  def rdk_consumers
    @consumer.instance_variable_get(:@consumers)
  end
end

module StatsThings
  def assigned_partitions
    latest_offsets
  end

  def latest_offsets
    @stats.last && offsets(@stats.last) || {}
  end

  def statistics_callback(stats)
    # https://docs.confluent.io/5.0.4/clients/librdkafka/md_STATISTICS.html
    return unless stats["type"] == "consumer"

    @stats ||= []
    @stats << stats
    output = offsets(stats).empty? ? stats.except("brokers") : offsets(stats)

    if @stats.length > 1 && @stats.last(2).map { |s| assignments(s) }.uniq.length > 1
      puts "assigments chaged!"
      p @stats.last(2).map { |s| assignments(s) }
    end
    # p "[#{Process.pid}] " + output.inspect
  end

  def offsets(stats)
    get_interesting_stats(stats)
      .fetch(:partitions_by_topic)
      .map { |topic, partition_infos|
        partition_infos.map { |info| info.slice("partition", "stored_offset", "committed_offset") }
      }
  end

  def get_interesting_stats(stats)
    $consumer_stats = stats
    partitions_by_topic = stats["topics"].map { |name, topic_info|
      ps = topic_info.fetch("partitions")

      partition_array = ps.keys
        .select { |pn| pn.to_i > -1 }
        .map { |pn| ps[pn.to_s] }

      [name, partition_array]
    }.to_h

    {
      rebalances: stats.dig("cgrp"," rebalance_cnt"),
      rebalanced_at: stats.dig("cgrp", "rebalance_age"),
      partitions_by_topic: partitions_by_topic,
    }
  rescue KeyError => e
    p e
    p stats.keys
  end

  def register_at_exit_hook
    return if @__at_exit_registered
    at_exit do
      jsons = @stats.map { |h| JSON.dump(h) }
      File.open("tmp/stats.jsons", "w") { |f| f.puts(jsons) }

      message_count = @message_count_by_partition.values.flatten.sum
      puts "EXIT REPORT"
      puts "Received #{message_count} messages"
      puts "Received #{@stats.count} stats"
      puts "Last offsets: " + latest_offsets.inspect
      puts "message_counts = " + @message_count_by_partition.to_s
    end
    @__at_exit_registered = true
  end

  def assignments(stats)
    stats["brokers"]
      .values
      .map { |b| b["toppars"] }.first.values
      .map { |tp| tp["partition"] }
  end

  def teardown
    super

    penultimate_stats_count = @stats.count
    debug "~~~~~~ teardown start 🏁 stats count = " + penultimate_stats_count.to_s
    # sleep 1
    # @consumer.commit

    Timeout.timeout(10) do
      until @stats.count > penultimate_stats_count
        sleep 0.05
      end
    end
    puts ""
    debug "~~~~~~ teardown end ✅ stats count = " + @stats.count
  rescue Timeout::Error
    debug "~~~~~~ teardown ERROR ❌ timed out waiting for stats 😭"
  ensure
    super
  end
end

class MinMax
  def initialize
    @min = Float::INFINITY
    @max = 0
  end

  attr_reader :min, :max

  def <<(new_value)
    if new_value > @max
      @max = new_value
    end
    if new_value < @min
      @min = new_value
    end
    new_value
  end

  def to_s
    to_a.to_s
  end

  def to_a
    [@min, @max]
  end

  def to_h
    { min: @min, max: @max }
  end
end
}

