# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

# Simple migration to create two tables for the sample application.
class CreateTables < ActiveRecord::Migration[7.0]
  def change
    create_table :singers, id: false, primary_key: :singer_id do |t|
      t.string :singer_id, limit: 36, null: false, primary_key: true
      t.string :first_name, limit: 100
      t.string :last_name, limit: 200, null: false
      t.virtual :full_name, type: :string, as: "coalesce(concat(first_name, ' '::varchar, last_name), last_name)", stored: true
      t.integer :lock_version, null: false
    end

    create_table :albums, id: false, primary_key: :album_id do |t|
      t.string :album_id, limit: 36, null: false, primary_key: true
      t.string :title
      # We do not include an index on the foreign key because Cloud Spanner
      # will automatically create a managed index for the foreign key
      # constraint.
      t.references :singer, foreign_key: {primary_key: :singer_id},
                   type: :string, limit: 36, index: false
    end
  end
end
