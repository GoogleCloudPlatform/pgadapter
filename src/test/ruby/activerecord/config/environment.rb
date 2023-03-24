# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require 'active_record'
require 'bundler'

Bundler.require

ActiveRecord::Base.establish_connection(
  adapter: 'postgresql',
  database: 'activerecord',
  host: ENV['PGHOST'] || 'localhost',
  port: ENV['PGPORT'] || '5432',
  username: ENV['PGUSER'],
  password: ENV['PGPASSWORD'],
  pool: 5,
  advisory_locks: false,
)
