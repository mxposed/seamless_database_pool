module ActiveRecord
  module ConnectionAdapters
    class NoVerifyConnectionPool < ConnectionPool
      def checkout_and_verify(c)
        c
      end
    end
  end
end
