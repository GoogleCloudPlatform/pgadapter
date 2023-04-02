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

    create_table :concerts, id: false, primary_key: :venue_id do |t|
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

  end
end
