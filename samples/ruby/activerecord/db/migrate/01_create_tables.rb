# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

# Simple migration to create the schema for the sample application.
class CreateTables < ActiveRecord::Migration[7.0]
  def change
    create_table :singers, id: false, primary_key: :singer_id do |t|
      t.string :singer_id, limit: 36, null: false, primary_key: true
      t.string :first_name, limit: 100
      t.string :last_name, limit: 200, null: false
      t.virtual :full_name, type: :string, as: "coalesce(concat(first_name, ' '::varchar, last_name), last_name)", stored: true
      t.boolean :active

      t.datetime :created_at
      t.datetime :updated_at
      t.integer :lock_version, null: false
    end

    create_table :albums, id: false, primary_key: :album_id do |t|
      t.string :album_id, limit: 36, null: false, primary_key: true
      t.string :title
      t.numeric :marketing_budget
      t.date :release_date
      t.binary :cover_picture
      # We do not include an index on the foreign key because Cloud Spanner
      # will automatically create a managed index for the foreign key
      # constraint.
      t.references :singer, foreign_key: {primary_key: :singer_id},
                   type: :string, limit: 36, index: false

      t.datetime :created_at
      t.datetime :updated_at
      t.integer :lock_version, null: false
    end

    # The ActiveRecord PostgreSQL provider does not know how to create an interleaved table.
    # These tables must therefore be created using a hand-written SQL script.
    # Note that interleaved tables require you to use a composite primary key.
    # This also requires "gem 'composite_primary_keys', '~> 14'" to be part of your project.
    execute "create table tracks (
        album_id     varchar(36) not null,
        track_number bigint not null,
        title        varchar not null,
        sample_rate  float8 not null,
        created_at   timestamptz,
        updated_at   timestamptz,
        lock_version bigint not null,
        primary key (album_id, track_number)
    ) interleave in parent albums on delete cascade;
    "

    create_table :venues, id: false, primary_key: :venue_id do |t|
      t.string :venue_id, limit: 36, null: false, primary_key: true
      t.string :name
      t.jsonb :description
      t.datetime :created_at
      t.datetime :updated_at
      t.integer :lock_version, null: false
    end

    create_table :concerts, id: false, primary_key: :concert_id do |t|
      t.string :concert_id, limit: 36, null: false, primary_key: true
      t.references :venue, foreign_key: {primary_key: :venue_id},
                   type: :string, limit: 36, index: false
      t.references :singer, foreign_key: {primary_key: :singer_id},
                   type: :string, limit: 36, index: false
      t.string :name
      t.datetime :start_time, null: false
      t.datetime :end_time, null: false
      t.datetime :created_at
      t.datetime :updated_at
      t.integer :lock_version, null: false
      t.check_constraint "end_time > start_time", name: :chk_end_time_after_start_time
    end

    # The ActiveRecord PostgreSQL provider does not know how to create a bit-reversed sequence.
    # These sequences must therefore be created using a hand-written SQL script.
    # Bit-reversed sequences can be used to create auto-generated primary key values that are
    # safe from hot-spotting.
    # See https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots
    # for more information on choosing a good primary key.
    execute "create sequence ticket_sale_seq
             bit_reversed_positive
             skip range 1 1000
             start counter with 50000;"

    # Create a table that uses a bit-reversed sequence to auto-generate primary key values.
    # The 'id' column definition is specified using a SQL statement to ensure that we get
    # a column with the right type, and with a default clause that selects a value from the
    # sequence.
    create_table :ticket_sales, id: false, primary_key: :id do |t|
      t.column :id, "bigint not null primary key default nextval('ticket_sale_seq')"
      t.references :concert, foreign_key: {primary_key: :concert_id},
                   type: :string, limit: 36, index: false
      t.string :customer_name
      t.numeric :price
      t.string :seats, array: true
      t.datetime :created_at
      t.datetime :updated_at
      t.integer :lock_version, null: false
    end

  end
end
