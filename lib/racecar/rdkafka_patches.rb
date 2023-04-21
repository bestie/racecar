module Rdkafka::Bindings
  rebalance_callback = FFI::Function.new(
    :void, [:pointer, :int, :pointer, :pointer]
  ) do |client_ptr, code, partitions_ptr, opaque_ptr|
    case code
    when RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
      if Rdkafka::Bindings.rd_kafka_rebalance_protocol(client_ptr) == "COOPERATIVE"
        puts "&&&&&&&&&&&&&&&&&&&& incremental_assign"
        Rdkafka::Bindings.rd_kafka_incremental_assign(client_ptr, partitions_ptr)
      else
        puts "&&&&&&&&&&&&&&&&&&&& assign"
        Rdkafka::Bindings.rd_kafka_assign(client_ptr, partitions_ptr)
      end
    else # RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS or errors
      if Rdkafka::Bindings.rd_kafka_rebalance_protocol(client_ptr) == "COOPERATIVE"
        puts "&&&&&&&&&&&&&&&&&&&& incremental_unassign"
        Rdkafka::Bindings.rd_kafka_incremental_unassign(client_ptr, partitions_ptr)
      else
        Rdkafka::Bindings.rd_kafka_assign(client_ptr, FFI::Pointer::NULL)
      end
    end

    opaque = Rdkafka::Config.opaques[opaque_ptr.to_i]
    return unless opaque

    tpl = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(partitions_ptr).freeze
    consumer = Rdkafka::Consumer.new(client_ptr)

    begin
      case code
      when RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
        opaque.call_on_partitions_assigned(consumer, tpl)
      when RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
        opaque.call_on_partitions_revoked(consumer, tpl)
      end
    rescue Exception => err
      Rdkafka::Config.logger.error("Unhandled exception: #{err.class} - #{err.message}")
    end
  end

  # new functions for coop rebalancing
  attach_function :rd_kafka_incremental_assign, [:pointer, :pointer], :int
  attach_function :rd_kafka_incremental_unassign, [:pointer, :pointer], :int
  attach_function :rd_kafka_rebalance_protocol, [:pointer], :string

  remove_const(:RebalanceCallback)
  const_set(:RebalanceCallback, rebalance_callback)
end
