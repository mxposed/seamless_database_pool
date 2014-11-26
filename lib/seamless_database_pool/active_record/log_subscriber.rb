module SeamlessDatabasePool
  module LogSubscriber
    def self.included(base)
      base.send(:attr_accessor, :connection_name)
      base.alias_method_chain :sql, :connection_name
      base.alias_method_chain :debug, :connection_name
    end

    def sql_with_connection_name(event)
      self.connection_name = event.payload[:connection_name]
      sql_without_connection_name(event)
    end

    def debug_with_connection_name(msg)
      if connection_name == :master
        colour = ::ActiveSupport::LogSubscriber::RED
      else
        colour = ::ActiveSupport::LogSubscriber::BLUE
      end
      conn = connection_name ? color("[#{connection_name}]", colour, true) : ''
      debug_without_connection_name(conn + msg)
    end
  end

  ::ActiveRecord::LogSubscriber.send(:include, LogSubscriber)
end

