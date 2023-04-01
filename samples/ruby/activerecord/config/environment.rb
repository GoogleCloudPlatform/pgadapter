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
# You only need this if your application:
# 1. Executes migrations OR
# 2. Creates the `schema_migrations` and/or `ar_internal_metadata` tables.
ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.datetime_type = :timestamptz
module ActiveRecord::ConnectionAdapters
  class PostgreSQLAdapter
    def supports_datetime_with_precision?
      false
    end
  end
end
