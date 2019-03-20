module Blazer
  class RunStatementJob < ApplicationJob
    queue_as :blazer
    
    def perform(data_source_id, statement, options)
      data_source = Blazer.data_sources[data_source_id]
      begin
        Blazer::RunStatement.new.perform(data_source, statement, options)
      rescue Exception => e
        Blazer::Result.new(data_source, [], [], "Unknown error", nil, false)
        Blazer.cache.write(data_source.run_cache_key(options[:run_id]), Marshal.dump([[], [], "Unknown error", nil]), expires_in: 30.seconds)
        raise e
      end
    end
  end
end
