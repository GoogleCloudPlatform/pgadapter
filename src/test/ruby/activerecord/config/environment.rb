# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require 'active_record'
require 'bundler'

Bundler.require

# Make sure that the PostgreSQL-adapter uses timestamptz without any type modifiers.
ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.datetime_type = :timestamptz
module ActiveRecord::ConnectionAdapters
  class PostgreSQLAdapter
    def supports_datetime_with_precision?
      false
    end
  end
end

ActiveRecord::Base.establish_connection(
  adapter: 'postgresql',
  database: ENV['PGDATABASE'] || 'activerecord',
  host: ENV['PGHOST'] || 'localhost',
  port: ENV['PGPORT'] || '5432',
  username: ENV['PGUSER'],
  password: ENV['PGPASSWORD'],
  pool: 5,
  advisory_locks: false,
  variables: {'spanner.ddl_transaction_mode': :AutocommitExplicitTransaction},
)
