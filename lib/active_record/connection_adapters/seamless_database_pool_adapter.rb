module ActiveRecord
  class Base
    class << self
      def seamless_database_pool_connection(config)
        pool_weights = {}

        config = config.with_indifferent_access
        default_config = {:pool_weight => 1}.merge(config.merge(:adapter => config[:pool_adapter])).with_indifferent_access
        default_config.delete(:master)
        default_config.delete(:read_pool)
        default_config.delete(:pool_adapter)

        master_config = default_config.merge(config[:master]).with_indifferent_access
        master_config[:connection_name] = :master
        master_config[:pool] = 1
        establish_adapter(master_config[:adapter])
        if defined? ActiveRecord::ConnectionAdapters::ConnectionSpecification
          spec_class = ActiveRecord::ConnectionAdapters::ConnectionSpecification
        else
          spec_class = ActiveRecord::Base::ConnectionSpecification
        end

        master_connection = ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec_class.new(master_config, "#{master_config[:adapter]}_connection"))
        pool_weights[master_connection] = master_config[:pool_weight].to_i if master_config[:pool_weight].to_i > 0

        read_connections = []
        config[:read_pool].each_with_index do |read_config, i|
          read_config = default_config.merge(read_config).with_indifferent_access
          read_config[:pool_weight] = read_config[:pool_weight].to_i
          if read_config[:pool_weight] > 0
            establish_adapter(read_config[:adapter])
            read_config[:connection_name] = "slave##{i + 1}".to_sym
            read_config[:pool] = 1

            conn = ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec_class.new(read_config, "#{read_config[:adapter]}_connection"))
            read_connections << conn
            pool_weights[conn] = read_config[:pool_weight]
          end
        end if config[:read_pool]

        adapter_class = SeamlessDatabasePool.adapter_class_for(master_config[:adapter])
        klass = ::ActiveRecord::ConnectionAdapters::SeamlessDatabasePoolAdapter.adapter_class(adapter_class)
        klass.new(nil, logger, master_connection, read_connections, pool_weights)
      end

      def establish_adapter(adapter)
        raise AdapterNotSpecified.new("database configuration does not specify adapter") unless adapter
        raise AdapterNotFound.new("database pool must specify adapters") if adapter == 'seamless_database_pool'

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

        adapter_method = "#{adapter}_connection"
        unless respond_to?(adapter_method)
          raise AdapterNotFound, "database configuration specifies nonexistent #{adapter} adapter"
        end
      end
    end

    module SeamlessDatabasePoolBehavior
      def self.included(base)
        base.alias_method_chain(:reload, :seamless_database_pool)
      end

      # Force reload to use the master connection since it's probably being called for a reason.
      def reload_with_seamless_database_pool(*args)
        SeamlessDatabasePool.use_master_connection do
          reload_without_seamless_database_pool(*args)
        end
      end
    end

    include(SeamlessDatabasePoolBehavior) unless include?(SeamlessDatabasePoolBehavior)
  end

  module ConnectionAdapters
    class SeamlessDatabasePoolAdapter < AbstractAdapter

      attr_reader :read_connections

      class << self
        # Create an anonymous class that extends this one and proxies methods to the pool connections.
        def adapter_class(klass)
          # Define methods to proxy to the appropriate pool
          read_only_methods = [:select, :select_rows, :execute, :tables, :columns, :exec_query]
          clear_cache_methods = [:insert, :update, :delete]
          master_methods = []
          adapter_class_name = klass.name.demodulize
          return const_get(adapter_class_name) if const_defined?(adapter_class_name, false)


          # Get a list of all methods redefined by the underlying adapter. These will be
          # proxied to the master connection.
          override_classes = (klass.ancestors - AbstractAdapter.ancestors)
          override_classes.each do |connection_class|
            master_methods.concat(connection_class.public_instance_methods(false))
            master_methods.concat(connection_class.protected_instance_methods(false))
          end
          master_methods = master_methods.collect{|m| m.to_sym}.uniq
          master_methods -= public_instance_methods(false) + protected_instance_methods(false) + private_instance_methods(false)
          master_methods -= read_only_methods
          master_methods -= [:select_all, :select_one, :select_value, :select_values]
          master_methods -= clear_cache_methods

          klass = Class.new(self)
          master_methods.each do |method_name|
            klass.class_eval <<-EOS, __FILE__, __LINE__ + 1
              def #{method_name}(*args, &block)
                connection = current_read_connection
                proxy_connection_method(connection, :#{method_name}, :master, *args, &block)
              end
            EOS
          end

          clear_cache_methods.each do |method_name|
            klass.class_eval <<-EOS, __FILE__, __LINE__ + 1
              def #{method_name}(*args, &block)
                clear_query_cache if query_cache_enabled
                connection = current_read_connection
                proxy_connection_method(connection, :#{method_name}, :master, *args, &block)
              end
            EOS
          end

          read_only_methods.each do |method_name|
            klass.class_eval <<-EOS, __FILE__, __LINE__ + 1
              def #{method_name}(*args, &block)
                connection = current_read_connection
                proxy_connection_method(connection, :#{method_name}, :read, *args, &block)
              end
            EOS
          end
          klass.send :protected, :select

          const_set(adapter_class_name, klass)

          klass
        end

        # Set the arel visitor on the connections.
        def visitor_for(pool)
          # This is ugly, but then again, so is the code in ActiveRecord for setting the arel
          # visitor. There is a note in the code indicating the method signatures should be updated.
          config = pool.spec.config.with_indifferent_access
          adapter = config[:master][:adapter] || config[:pool_adapter]
          SeamlessDatabasePool.adapter_class_for(adapter).visitor_for(pool)
        end
      end

      def initialize(connection, logger, master_connection, read_connections, pool_weights)
        @master_connection = master_connection
        @read_connections = read_connections.dup.freeze

        super(connection, logger)

        @weighted_read_connections = []
        pool_weights.each_pair do |conn, weight|
          weight.times{@weighted_read_connections << conn}
        end
        @available_read_connections = [AvailableConnections.new(@weighted_read_connections)]
      end

      def adapter_name #:nodoc:
        'Seamless_Database_Pool'
      end

      # Returns an array of the master connection and the read pool connections
      def all_connections
        [@master_connection] + @read_connections
      end

      # Get the pool weight of a connection
      def pool_weight(connection)
        return @weighted_read_connections.select{|conn| conn == connection}.size
      end

      def requires_reloading?
        false
      end

      def transaction(options = {})
        use_master_connection do
          super
        end
      end

      def visitor=(visitor)
        all_connections.each{|conn| conn.visitor = visitor}
      end

      def visitor(*args, &block)
        proxy_connection_method(master_connection, :visitor, :master, *args, &block)
      end

      def active?
        active = true
        do_to_connections {|conn| active &= conn.active?}
        return active
      end

      def reconnect!
        do_to_connections {|conn| conn.reconnect!}
      end

      def disconnect!
        do_to_connections {|conn| conn.disconnect!}
      end

      def reset!
        do_to_connections {|conn| conn.reset!}
      end

      def verify!(*ignored)
        #do_to_connections {|conn| conn.verify!(*ignored)}
      end

      def reset_runtime
        total = 0.0
        do_to_connections {|conn| total += conn.reset_runtime}
        total
      end

      # Get a random read connection from the pool. If the connection is not active, it will attempt to reconnect
      # to the database. If that fails, it will be removed from the pool for one minute.
      def random_read_connection(is_backup=false)
        weighted_read_connections = available_read_connections
        if @use_master
          master_connection
        else
          if weighted_read_connections.empty?
            backup = SeamlessDatabasePool.backup_connection(self) unless is_backup
            return @available_read_connections.last.failed_connection unless backup
            return backup
          end
          weighted_read_connections[rand(weighted_read_connections.length)]
        end
      end

      # Get the current read connection
      def current_read_connection
        SeamlessDatabasePool.read_only_connection(self)
      end

      def using_master_connection?
        !!@use_master
      end

      # Force using the master connection in a block.
      def use_master_connection
        save_val = @use_master
        begin
          @use_master = true
          yield if block_given?
        ensure
          @use_master = save_val
        end
      end

      def to_s
        "#<#{self.class.name}:0x#{object_id.to_s(16)} #{all_connections.size} connections>"
      end

      def inspect
        to_s
      end

      class DatabaseConnectionError < StandardError
      end

      # This simple class puts an expire time on an array of connections. It is used so the a connection
      # to a down database won't try to reconnect over and over.
      class AvailableConnections
        attr_reader :connections, :failed_connection
        attr_writer :expires

        def initialize(connections, failed_connection = nil, expires = nil)
          @connections = connections
          @failed_connection = failed_connection
          @expires = expires
        end

        def expired?
          @expires ? @expires <= Time.now : false
        end

        def reconnect!
          success = nil
          failed_connection.with_connection { |conn|
            conn.reconnect!
            success = conn.active?
          }
          raise DatabaseConnectionError.new unless success
        end
      end

      # Get the available weighted connections. When a connection is dead and cannot be reconnected, it will
      # be temporarily removed from the read pool so we don't keep trying to reconnect to a database that isn't
      # listening.
      def available_read_connections
        available = @available_read_connections.last
        if available.expired?
          begin
            @logger.info("Adding dead database connection back to the pool") if @logger
            available.reconnect!
          rescue => e
            # Couldn't reconnect so try again in a little bit
            if @logger
              @logger.warn("Failed to reconnect to database when adding connection back to the pool")
              @logger.warn(e)
            end
            available.expires = 30.seconds.from_now
            return available.connections
          end

          # If reconnect is successful, the connection will have been re-added to @available_read_connections list,
          # so let's pop this old version of the connection
          @available_read_connections.pop

          # Now we'll try again after either expiring our bad connection or re-adding our good one
          available_read_connections
        else
          available.connections
        end
      end

      def reset_available_read_connections
        @available_read_connections.slice!(1, @available_read_connections.length)
        @available_read_connections.first.connections.each do |connection|
          unless connection.active?
            connection.reconnect! rescue nil
          end
        end
      end

      def master_connection
        if @master_expire and @master_expire <= Time.now
          @master_connection.disconnect!
          @master_expire = nil
        end
        @master_connection
      end

      def suppress_master_connection(expire)
        @master_expire = expire.seconds.from_now
        @logger.warn("Suppressing master connection for #{expire} seconds") if @logger
      end

      # Temporarily remove a connection from the read pool.
      def suppress_read_connection(conn, expire)
        available = available_read_connections
        connections = available.reject{|c| c == conn}
        SeamlessDatabasePool.reject_read_connection(self, conn)

        # This wasn't a read connection so don't suppress it
        return if connections.length == available.length


        @logger.warn("Removing #{conn.inspect} from the connection pool for #{expire} seconds") if @logger
        # Available connections will now not include the suppressed connection for a while
        @available_read_connections.push(AvailableConnections.new(connections, conn, expire.seconds.from_now))
      end

      private

      RECONNECT_PATTERN = /(not connected|Can't connect to MySQL|MySQL server has gone away|Lost connection to MySQL server|Packet too large)/i

      def proxy_connection_method(connection_pool, method, proxy_type, *args, &block)
        if connection_pool == @master_connection and @master_expire
          connection_pool = alternative_connection(connection_pool, method, proxy_type, *args, &block)
        end
        retry_ok = true
        begin
          connection_pool.with_connection { |connection|
            connection.send(method, *args, &block)
          }
        rescue => e
          @logger.warn(e) if @logger
          if retry_ok and e.message =~ RECONNECT_PATTERN
            @logger.warn('RECONNECT !! ! ! ! ! !') if @logger
            connection_pool.disconnect!
            retry_ok = false
            retry
          end
          if proxy_type == :read and connection_pool != @master_connection
            suppress_read_connection(connection_pool, 30)
            connection_pool = alternative_connection(connection_pool, method, proxy_type, *args, &block)
            raise e unless connection_pool
            SeamlessDatabasePool.set_persistent_read_connection(self, connection_pool)
            proxy_connection_method(connection_pool, method, :retry, *args, &block)
          elsif connection_pool == @master_connection and (proxy_type == :read or proxy_type == :master)
            suppress_master_connection(5)
            connection_pool = alternative_connection(connection_pool, method, proxy_type, *args, &block)
            raise e unless connection_pool
            SeamlessDatabasePool.set_persistent_read_connection(self, connection_pool)
            proxy_connection_method(connection_pool, method, :retry, *args, &block)
          else
            raise e
          end
        end
      end

      def alternative_connection(connection_pool, method, proxy_type, *args, &block)
        if proxy_type == :read and connection_pool != @master_connection
          current_read_connection
        elsif connection_pool == @master_connection and (proxy_type == :read or proxy_type == :master)
          SeamlessDatabasePool.backup_connection(self)
        end
      end

      # Yield a block to each connection in the pool. If the connection is dead, ignore the error
      # unless it is the master connection
      def do_to_connections
        all_connections.each do |conn|
          begin
            yield(conn)
          rescue => e
            raise e if conn == master_connection
          end
        end
        nil
      end
    end
  end
end
