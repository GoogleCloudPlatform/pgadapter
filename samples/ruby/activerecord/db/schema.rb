# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[7.0].define(version: 1) do
  create_table "albums", primary_key: "album_id", id: { type: :string, limit: 36 }, comment: "", force: :cascade do |t|
    t.string "title"
    t.decimal "marketing_budget"
    t.date "release_date"
    t.binary "cover_picture"
    t.string "singer_id", limit: 36
    t.datetime "created_at", precision: nil
    t.datetime "updated_at", precision: nil
    t.bigint "lock_version", null: false
    t.index [], name: "IDX_albums_singer_id_N_90CD66D06B92DBCF"
  end

  create_table "concerts", primary_key: "concert_id", id: { type: :string, limit: 36 }, comment: "", force: :cascade do |t|
    t.string "venue_id", limit: 36
    t.string "singer_id", limit: 36
    t.string "name"
    t.datetime "start_time", precision: nil, null: false
    t.datetime "end_time", precision: nil, null: false
    t.datetime "created_at", precision: nil
    t.datetime "updated_at", precision: nil
    t.bigint "lock_version", null: false
    t.index [], name: "IDX_concerts_singer_id_N_E92CCCE963BE681C"
    t.index [], name: "IDX_concerts_venue_id_N_F08F98ABB662E6D5"
    t.check_constraint nil, name: "chk_end_time_after_start_time"
  end

  create_table "singers", primary_key: "singer_id", id: { type: :string, limit: 36 }, comment: "", force: :cascade do |t|
    t.string "first_name", limit: 100
    t.string "last_name", limit: 200, null: false
    t.virtual "full_name", type: :string, as: "COALESCE(concat(first_name, ' '::character varying, last_name), last_name)", stored: true
    t.boolean "active"
    t.datetime "created_at", precision: nil
    t.datetime "updated_at", precision: nil
    t.bigint "lock_version", null: false
  end

  create_table "tracks", primary_key: ["album_id", "track_number"], comment: "", force: :cascade do |t|
    t.string "album_id", limit: 36, null: false
    t.bigint "track_number", null: false
    t.string "title", null: false
    t.float "sample_rate", null: false
    t.datetime "created_at", precision: nil
    t.datetime "updated_at", precision: nil
    t.bigint "lock_version", null: false
  end

  create_table "venues", primary_key: "venue_id", id: { type: :string, limit: 36 }, comment: "", force: :cascade do |t|
    t.string "name"
    t.jsonb "description"
    t.datetime "created_at", precision: nil
    t.datetime "updated_at", precision: nil
    t.bigint "lock_version", null: false
  end

  add_foreign_key "albums", "singers", primary_key: "singer_id"
  add_foreign_key "concerts", "singers", primary_key: "singer_id"
  add_foreign_key "concerts", "venues", primary_key: "venue_id"
end
