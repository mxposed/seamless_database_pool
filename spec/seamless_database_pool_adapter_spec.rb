require 'spec_helper'

module SeamlessDatabasePool
  class MockConnection < ActiveRecord::ConnectionAdapters::AbstractAdapter
    def initialize (name)
      @name = name
      super
    end

    def inspect
      "#{@name} connection"
    end

    def reconnect!
      sleep(0.1)
    end

    def active?
      true
    end

    def begin_db_transaction
    end

    def commit_db_transaction
    end
  end

  class MockMasterConnection < MockConnection
    def insert (sql, name = nil); end
    def update (sql, name = nil); end
    def execute (sql, name = nil); end
    def columns (table_name, name = nil); end
  end
end

module ActiveRecord
  class Base
    def self.writer_connection
    end

    def self.reader_connection
    end
  end
end

describe 'SeamlessDatabasePoolAdapter ActiveRecord::Base extension' do

  it 'should establish the connections in the pool merging global options into the connection options' do
    options = {
      :adapter => 'seamless_database_pool',
      :pool_adapter => 'reader',
      :username => 'user',
      :master => {
        'adapter' => 'writer',
        'host' => 'master_host'
      },
      :read_pool => [
        {'host' => 'read_host_1'},
        {'host' => 'read_host_2', 'pool_weight' => '2'},
        {'host' => 'read_host_3', 'pool_weight' => '0'}
      ]
    }

    pool_connection = double(:connection)
    master_connection = SeamlessDatabasePool::ConnectionInitializer.pool_for('adapter' => 'writer', 'host' => 'master_host', 'username' => 'user', 'pool_weight' => 1)
    read_connection_1 = SeamlessDatabasePool::ConnectionInitializer.pool_for('adapter' => 'reader', 'host' => 'read_host_1', 'username' => 'user', 'pool_weight' => 1)
    read_connection_2 = SeamlessDatabasePool::ConnectionInitializer.pool_for('adapter' => 'reader', 'host' => 'read_host_2', 'username' => 'user', 'pool_weight' => 2)
    logger = ActiveRecord::Base.logger
    weights = {master_connection => 1, read_connection_1 => 1, read_connection_2 => 2}

    SeamlessDatabasePool::ConnectionInitializer.should_receive(:pool_for).with('pool_weight' => 1, 'blacklist' => 30, 'adapter' => 'writer', 'username' => 'user', 'host' => 'master_host', 'connection_name' => :master, 'pool' => 1).and_return(master_connection)
    SeamlessDatabasePool::ConnectionInitializer.should_receive(:pool_for).with('pool_weight' => 1, 'blacklist' => 30, 'adapter' => 'reader', 'username' => 'user', 'host' => 'read_host_1', 'connection_name' => :'slave#1', 'pool' => 1).and_return(read_connection_1)
    SeamlessDatabasePool::ConnectionInitializer.should_receive(:pool_for).with('pool_weight' => 2, 'blacklist' => 30, 'adapter' => 'reader', 'username' => 'user', 'host' => 'read_host_2', 'connection_name' => :'slave#2', 'pool' => 1).and_return(read_connection_2)

    klass = double(:class)
    SeamlessDatabasePool.should_receive(:adapter_class_for).with(options[:master]['adapter']).and_return(SeamlessDatabasePool::MockConnection)
    ActiveRecord::ConnectionAdapters::SeamlessDatabasePoolAdapter.should_receive(:adapter_class).with(SeamlessDatabasePool::MockConnection).and_return(klass)
    klass.should_receive(:new).with(nil, logger, master_connection, [read_connection_1, read_connection_2], weights).and_return(pool_connection)

    ActiveRecord::Base.seamless_database_pool_connection(options).should == pool_connection
  end

  it 'should raise an error if the adapter would be recursive' do
    lambda{ActiveRecord::Base.seamless_database_pool_connection('seamless_database_pool').should_raise(ActiveRecord::AdapterNotFound)}
  end
end

describe 'SeamlessDatabasePoolAdapter' do

  let(:master_connection){ SeamlessDatabasePool::ConnectionInitializer.pool_for('pool_weight' => 1, :blacklist => 30, :adapter => 'writer', 'username' => 'user', 'host' => 'master_host', 'connection_name' => :master, :pool => 1) }
  let(:read_connection_1){ SeamlessDatabasePool::ConnectionInitializer.pool_for('pool_weight' => 1, :blacklist => 30, :adapter => 'reader', 'username' => 'user', 'host' => 'read_host_1', 'connection_name' => :'slave#1', :pool => 1) }
  let(:read_connection_2){ SeamlessDatabasePool::ConnectionInitializer.pool_for('pool_weight' => 1, :blacklist => 30, :adapter => 'reader', 'username' => 'user', 'host' => 'read_host_2', 'connection_name' => :'slave#2', :pool => 1) }
  let(:pool_connection) do
    weights = {master_connection => 1, read_connection_1 => 1, read_connection_2 => 2}
    connection_class = ActiveRecord::ConnectionAdapters::SeamlessDatabasePoolAdapter.adapter_class(SeamlessDatabasePool::MockMasterConnection)
    connection_class.new(nil, nil, master_connection, [read_connection_1, read_connection_2], weights)
  end

  it 'should be able to be converted to a string' do
    pool_connection.to_s.should =~ /\A#<ActiveRecord::ConnectionAdapters::SeamlessDatabasePoolAdapter::MockMasterConnection:0x[0-9a-f]+ 3 connections>\z/
    pool_connection.inspect.should == pool_connection.to_s
  end

  context 'selecting a connection from the pool' do
    it 'should initialize the connection pool' do
      pool_connection.master_connection.should == master_connection
      pool_connection.read_connections.should == [read_connection_1, read_connection_2]
      pool_connection.all_connections.should == [master_connection, read_connection_1, read_connection_2]
      pool_connection.pool_weight(master_connection).should == 1
      pool_connection.pool_weight(read_connection_1).should == 1
      pool_connection.pool_weight(read_connection_2).should == 2
    end

    it 'should return the current read connection' do
      SeamlessDatabasePool.should_receive(:read_only_connection).with(pool_connection).and_return(:current)
      pool_connection.current_read_connection.should == :current
    end

    it 'should select a random read connection' do
      mock_connection = double(:connection)
      mock_connection.stub(:active? => true)
      pool_connection.should_receive(:available_read_connections).and_return([:fake1, :fake2, mock_connection])
      pool_connection.should_receive(:rand).with(3).and_return(2)
      pool_connection.random_read_connection.should == mock_connection
    end

    it 'should not select the master connection if the read pool is empty' do
      pool_connection.should_receive(:available_read_connections).and_return([])
      pool_connection.random_read_connection.should == nil
    end

    it 'should select the master connection if the read pool is empty and we have backup' do
      SeamlessDatabasePool.should_receive(:backup_connection).with(pool_connection).and_return(master_connection)
      pool_connection.should_receive(:available_read_connections).and_return([])
      pool_connection.random_read_connection.should == master_connection
    end

    it 'should use the master connection in a block' do
      connection_class = ActiveRecord::ConnectionAdapters::SeamlessDatabasePoolAdapter.adapter_class(SeamlessDatabasePool::MockMasterConnection)
      connection = connection_class.new(nil, double(:logger), master_connection, [read_connection_1], {read_connection_1 => 1})
      connection.random_read_connection.should == read_connection_1
      connection.use_master_connection do
        connection.random_read_connection.should == master_connection
      end
      connection.random_read_connection.should == read_connection_1
    end

    it 'should use the master connection inside a transaction' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      read_conn = SeamlessDatabasePool::MockConnection.new('slave')

      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)

      connection_class = ActiveRecord::ConnectionAdapters::SeamlessDatabasePoolAdapter.adapter_class(SeamlessDatabasePool::MockMasterConnection)
      connection = connection_class.new(nil, double(:logger), master_connection, [read_connection_1], {read_connection_1 => 1})
      master_conn.should_receive(:begin_db_transaction)
      master_conn.should_receive(:commit_db_transaction)
      master_conn.should_receive(:select).with('Transaction SQL', nil)
      read_conn.should_receive(:select).with('SQL 1', nil)
      read_conn.should_receive(:select).with('SQL 2', nil)

      SeamlessDatabasePool.use_persistent_read_connection do
        connection.send(:select, 'SQL 1', nil)
        connection.transaction do
          connection.send(:select, 'Transaction SQL', nil)
        end
        connection.send(:select, 'SQL 2', nil)
      end
    end
  end

  context 'read connection methods' do
    it 'should proxy select methods to a read connection' do
      read_conn = SeamlessDatabasePool::MockConnection.new('slave')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)

      pool_connection.should_receive(:current_read_connection).and_return(read_connection_1)
      read_conn.should_receive(:select).with('SQL').and_return(:retval)
      pool_connection.send(:select, 'SQL').should == :retval
    end

    it 'should proxy execute methods to a read connection' do
      read_conn = SeamlessDatabasePool::MockConnection.new('slave')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)

      pool_connection.should_receive(:current_read_connection).and_return(read_connection_1)
      read_conn.should_receive(:execute).with('SQL').and_return(:retval)
      pool_connection.execute('SQL').should == :retval
    end

    it 'should proxy select_rows methods to a read connection' do
      read_conn = SeamlessDatabasePool::MockConnection.new('slave')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)

      pool_connection.should_receive(:current_read_connection).and_return(read_connection_1)
      read_conn.should_receive(:select_rows).with('SQL').and_return(:retval)
      pool_connection.select_rows('SQL').should == :retval
    end
  end

  context 'master connection methods' do
    it 'should proxy insert method to the master connection' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      master_conn.should_receive(:insert).with('SQL').and_return(:retval)
      pool_connection.insert('SQL').should == :retval
    end

    it 'should proxy update method to the master connection' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      master_conn.should_receive(:update).with('SQL').and_return(:retval)
      pool_connection.update('SQL').should == :retval
    end

    it 'should proxy columns method to the master connection' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      master_conn.should_receive(:columns).with(:table).and_return(:retval)
      pool_connection.columns(:table).should == :retval
    end
  end

  context 'fork to all connections' do
    it 'should fork active? to all connections and return true if all are up' do
      master_connection.should_receive(:active?).and_return(true)
      read_connection_1.should_receive(:active?).and_return(true)
      read_connection_2.should_receive(:active?).and_return(true)
      pool_connection.active?.should == true
    end

    it 'should fork active? to all connections and return false if one is down' do
      master_connection.should_receive(:active?).and_return(true)
      read_connection_1.should_receive(:active?).and_return(true)
      read_connection_2.should_receive(:active?).and_return(false)
      pool_connection.active?.should == false
    end

    it 'should ignore verify!' do
      master_connection.should_not_receive(:verify!)
      read_connection_1.should_not_receive(:verify!)
      read_connection_2.should_not_receive(:verify!)
      pool_connection.verify!
    end

    it 'should fork disconnect! to all connections' do
      master_connection.should_receive(:disconnect!)
      read_connection_1.should_receive(:disconnect!)
      read_connection_2.should_receive(:disconnect!)
      pool_connection.disconnect!
    end

    it 'should fork reconnect! to all connections' do
      master_connection.should_receive(:reconnect!)
      read_connection_1.should_receive(:reconnect!)
      read_connection_2.should_receive(:reconnect!)
      pool_connection.reconnect!
    end

    it 'should fork reset_runtime to all connections' do
      master_connection.should_receive(:reset_runtime).and_return(1)
      read_connection_1.should_receive(:reset_runtime).and_return(2)
      read_connection_2.should_receive(:reset_runtime).and_return(3)
      pool_connection.reset_runtime.should == 6
    end
  end

  context 'reconnection' do
    it 'should proxy requests to a connection' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      args = [:arg1, :arg2]
      block = Proc.new{}
      master_conn.should_receive(:select_value).with(*args, &block)
      # master_conn.should_not_receive(:active?)
      master_conn.should_not_receive(:reconnect!)
      pool_connection.send(:proxy_connection_method, master_connection, :select_value, :master, *args, &block)
    end

    it 'should try to reconnect dead connections when they become available again' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).twice.and_return(master_conn)

      master_conn.stub(:select).and_raise('SQL ERROR')
      master_conn.should_receive(:disconnect!)
      now = Time.now
      lambda { pool_connection.select_value('SQL') }.should raise_error('SQL ERROR')
      Time.stub(:now => now + 31)
      lambda { pool_connection.select_value('SQL') }.should raise_error('SQL ERROR')
    end

    it 'should not try to reconnect live connections' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      args = [:arg1, :arg2]
      block = Proc.new{}
      master_conn.should_receive(:select).with(*args, &block).and_raise('SQL ERROR')
      master_conn.should_not_receive(:disconnect!)
      lambda { pool_connection.send(:proxy_connection_method, master_connection, :select, :read, *args, &block) }.should raise_error('SQL ERROR')
    end

    it 'should not try to reconnect a connection during a retry' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      args = [:arg1, :arg2]
      block = Proc.new{}
      master_conn.should_receive(:select).with(*args, &block).and_raise('SQL ERROR')
      master_conn.should_not_receive(:disconnect!)
      lambda { pool_connection.send(:proxy_connection_method, master_connection, :select, :retry, *args, &block) }.should raise_error('SQL ERROR')
    end

    it 'should try to execute a read statement again after a connection error' do
      read_conn = SeamlessDatabasePool::MockConnection.new('read')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).twice.and_return(read_conn)
      SeamlessDatabasePool.set_read_only_connection_type(:persistent)

      connection_error = StandardError.new('Lost connection to MySQL server')
      pool_connection.should_receive(:current_read_connection).and_return(read_connection_1)
      read_conn.should_receive(:select).with('SQL').and_raise(connection_error)
      read_conn.should_receive(:disconnect!)
      pool_connection.should_not_receive(:suppress_read_connection)
      SeamlessDatabasePool.should_not_receive(:set_persistent_read_connection)
      read_conn.should_receive(:select).with('SQL').and_return(:results)
      pool_connection.send(:select, 'SQL').should == :results
    end

    it 'should not try to execute a read statement again after a connection error if the master connection must be used' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      master_conn.should_receive(:select).with('SQL').and_raise('Fail')
      pool_connection.use_master_connection do
        lambda { pool_connection.send(:select, 'SQL') }.should raise_error('Fail')
      end
    end

    it 'should not try to execute a read statement again after a non-connection error' do
      pool_connection.should_receive(:current_read_connection).and_return(read_connection_1)
      pool_connection.should_receive(:proxy_connection_method).with(read_connection_1, :select, :read, 'SQL').and_raise('SQL Error')
      lambda { pool_connection.send(:select, 'SQL') }.should raise_error('SQL Error')
    end

    it 'should use a different connection on a retry if the original connection could not be reconnected' do
      read_conn = SeamlessDatabasePool::MockConnection.new('read')
      read_conn2 = SeamlessDatabasePool::MockConnection.new('read')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_2.spec.config).and_return(read_conn2)

      pool_connection.should_receive(:current_read_connection).and_return(read_connection_1, read_connection_2)
      read_conn.should_receive(:select).with('SQL').and_raise('Fail')
      pool_connection.should_receive(:suppress_read_connection).with(read_connection_1, 30)
      SeamlessDatabasePool.should_receive(:set_persistent_read_connection).with(pool_connection, read_connection_2)
      read_conn2.should_receive(:select).with('SQL').and_return(:results)
      pool_connection.send(:select, 'SQL').should == :results
    end

    it "should keep track of read connections that can't be reconnected for a set period" do
      pool_connection.available_read_connections.should include(read_connection_1)
      pool_connection.suppress_read_connection(read_connection_1, 30)
      pool_connection.available_read_connections.should_not include(read_connection_1)
    end

    it 'should raise error on master connection when no backup' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      master_conn.should_receive(:select).with('SQL').and_raise('Fail')
      pool_connection.suppress_master_connection(30)
      expect { pool_connection.send(:select, 'SQL') }.to raise_error('Fail')
    end

    it 'should use backup on master connection' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      read_conn = SeamlessDatabasePool::MockConnection.new('read')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)

      read_conn.should_receive(:select).with('SQL').and_return(:results)
      pool_connection.suppress_read_connection(read_connection_2, 30)
      pool_connection.suppress_master_connection(30)
      pool_connection.should_receive(:available_read_connections).and_return([read_connection_1])
      SeamlessDatabasePool.set_backup_connection_type(:persistent) {
        pool_connection.send(:select, 'SQL').should == :results
      }
    end

    it 'should raise error on master connection when all connections are dead' do
      master_conn = SeamlessDatabasePool::MockMasterConnection.new('master')
      ActiveRecord::Base.should_receive(:writer_connection).with(master_connection.spec.config).and_return(master_conn)

      master_conn.should_receive(:select).with('SQL').and_raise('Fail')
      pool_connection.suppress_read_connection(read_connection_1, 30)
      pool_connection.suppress_read_connection(read_connection_2, 30)
      pool_connection.suppress_master_connection(30)
      SeamlessDatabasePool.set_backup_connection_type(:persistent) {
        expect { pool_connection.send(:select, 'SQL') }.to raise_error('Fail')
      }
    end

    it 'should return dead connections to the pool after the timeout has expired' do
      read_conn = SeamlessDatabasePool::MockConnection.new('read')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)

      pool_connection.available_read_connections.should include(read_connection_1)
      pool_connection.suppress_read_connection(read_connection_1, 0.2)
      pool_connection.available_read_connections.should_not include(read_connection_1)
      sleep(0.3)
      read_conn.should_receive(:reconnect!)
      read_conn.should_not_receive(:verify!)
      pool_connection.available_read_connections.should include(read_connection_1)
    end

    it 'should not return a connection to the pool until it can be reconnected' do
      read_conn = SeamlessDatabasePool::MockConnection.new('read')
      ActiveRecord::Base.should_receive(:reader_connection).with(read_connection_1.spec.config).and_return(read_conn)

      pool_connection.available_read_connections.should include(read_connection_1)
      pool_connection.suppress_read_connection(read_connection_1, 0.2)
      pool_connection.available_read_connections.should_not include(read_connection_1)
      sleep(0.3)
      read_conn.should_receive(:reconnect!)
      read_conn.should_not_receive(:verify!)
      read_conn.should_receive(:active?).and_return(false)
      pool_connection.available_read_connections.should_not include(read_connection_1)
    end

    it "should not try to suppress a read connection that wasn't available in the read pool" do
      stack = pool_connection.instance_variable_get(:@available_read_connections)
      stack.size.should == 1
      pool_connection.suppress_read_connection(read_connection_1, 30)
      stack.size.should == 2
      pool_connection.suppress_read_connection(read_connection_1, 30)
      stack.size.should == 2
    end
  end
end
