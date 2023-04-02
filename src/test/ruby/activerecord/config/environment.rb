# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require 'active_record'
require 'bundler'

Bundler.require

# Make sure that the PostgreSQL-adapter uses timestamptz by default, and without any type modifiers.

# The following sets the default type that should be used by PostgreSQL for datetime.
# This is only supported in ActiveRecord 7.0 and higher.
ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.datetime_type = :timestamptz

# Remove the above line and uncomment the line below if you are using ActiveRecord 6.1 or lower.
# ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::NATIVE_DATABASE_TYPES[:datetime] = { name: "timestamptz" }

# The following ensures that ActiveRecord does not use any type modifiers for timestamp types.
# That is; Cloud Spanner only supports `timestamptz` and not for example `timestamptz(6)`.
module ActiveRecord::ConnectionAdapters
  class PostgreSQLAdapter
    def supports_datetime_with_precision?
      false
    end
  end
end
