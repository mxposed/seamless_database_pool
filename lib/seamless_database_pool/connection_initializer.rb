module SeamlessDatabasePool
  module ConnectionInitializer
    def self.construct_connection(config, logger)
      pool_weights = {}

      config = config.with_indifferent_access
      default_config = {:pool_weight => 1, :blacklist => 30}.merge(config.merge(:adapter => config[:pool_adapter])).with_indifferent_access
      default_config.delete(:master)
      default_config.delete(:read_pool)
      default_config.delete(:pool_adapter)

      master_config = default_config.merge(config[:master]).with_indifferent_access
      master_config[:connection_name] = :master
      master_config[:pool] = 1
      establish_adapter(master_config[:adapter])

      master_pool = pool_for(master_config)
      pool_weights[master_pool] = master_config[:pool_weight].to_i if master_config[:pool_weight].to_i > 0

      read_pools = []
      config[:read_pool].each_with_index do |read_config, i|
        read_config = default_config.merge(read_config).with_indifferent_access
        read_config[:pool_weight] = read_config[:pool_weight].to_i
        if read_config[:pool_weight] > 0
          establish_adapter(read_config[:adapter])
          read_config[:connection_name] = "slave##{i + 1}".to_sym
          read_config[:pool] = 1

          conn = pool_for(read_config)
          read_pools << conn
          pool_weights[conn] = read_config[:pool_weight]
        end
      end if config[:read_pool]

      adapter_class = SeamlessDatabasePool.adapter_class_for(master_config[:adapter])
      klass = ActiveRecord::ConnectionAdapters::SeamlessDatabasePoolAdapter.adapter_class(adapter_class)
      klass.new(nil, logger, master_pool, read_pools, pool_weights)
    end

    def self.spec_class
      if defined? ActiveRecord::ConnectionAdapters::ConnectionSpecification
        ActiveRecord::ConnectionAdapters::ConnectionSpecification
      else
        ActiveRecord::Base::ConnectionSpecification
      end
    end

    def self.pool_for(config)
      ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec_class.new(config, "#{config[:adapter]}_connection"))
    end

    def self.establish_adapter(adapter)
      raise ActiveRecord::AdapterNotSpecified.new('database configuration does not specify adapter') unless adapter
      raise ActiveRecord::AdapterNotFound.new('database pool must specify adapters') if adapter == 'seamless_database_pool'

      adapter_method = "#{adapter}_connection"
      return if ActiveRecord::Base.respond_to?(adapter_method)

      begin
        require 'rubygems'
        gem "activerecord-#{adapter}-adapter"
        require "active_record/connection_adapters/#{adapter}_adapter"
      rescue LoadError
        begin
          require "active_record/connection_adapters/#{adapter}_adapter"
        rescue LoadError
          raise "Please install the #{adapter} adapter: `gem install activerecord-#{adapter}-adapter` (#{$!})"
        end
      end

      unless ActiveRecord::Base.respond_to?(adapter_method)
        raise AdapterNotFound, "database configuration specifies nonexistent #{adapter} adapter"
      end
    end
  end
end
