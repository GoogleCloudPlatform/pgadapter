# Copyright 2023 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "sinatra"
require "pg"

project = ENV["SPANNER_PROJECT"] || "my-project"
instance = ENV["SPANNER_INSTANCE"] || "my-instance"
database = ENV["SPANNER_DATABASE"] || "my-database"
qualified_database_name = "projects/#{project}/instances/#{instance}/databases/#{database}"

pgadpater_host = ENV["PGADAPTER_HOST"] || "localhost"
pgadpater_port = ENV["PGADAPTER_PORT"] || "5432"

set :bind, "0.0.0.0"
port = ENV["PORT"] || "8080"
set :port, port

get "/" do
  conn = PG.connect(dbname: qualified_database_name, host: pgadpater_host, port: pgadpater_port )
  conn.exec( "select 'Hello world!' as hello" ) do |result|
    result.each do |row|
      return "Greeting from Cloud Spanner PostgreSQL using ruby-pg: #{row["hello"]}\n"
    end
  end
  "No greeting returned by Cloud Spanner\n"
end
