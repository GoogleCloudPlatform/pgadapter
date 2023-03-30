# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require 'securerandom'
require_relative '../config/environment.rb'
require_relative '../models/singer'
require_relative '../models/album'
require_relative '../models/track'

first_names = %w[Pete Alice John Ethel Trudy Naomi Wendy]
last_names = %w[Wendelson Allison Peterson Johnson Henderson Ericsson Aronson]

adjectives = %w[daily happy blue generous cooked bad open]
nouns = %w[windows potatoes bank street tree glass bottle]

adverbs = %w[swiftly surprisingly rather closely furiously politely currently unimpressively gracefully abnormally stealthily irritably]
verbs = %w[poke rescue tick long soothe obtain fancy prick suffer encourage love own]

rnd = Random.new

Album.delete_all
Singer.delete_all

Singer.transaction do
  singers = []
  albums = []
  tracks = []
  10.times do
    singers.append({singer_id: SecureRandom.uuid,
                    first_name: first_names.sample,
                    last_name: last_names.sample,
                    active: true})
  end
  Singer.create singers

  30.times do
    singer_id = singers.sample[:singer_id]
    albums.append({album_id: SecureRandom.uuid,
                   title: "#{adjectives.sample} #{nouns.sample}",
                   singer_id: singer_id,
                   marketing_budget: rand * 10000,
                   release_date: Date.new(rand(100) + 1923, rand(12) + 1, rand(28) + 1),
                   cover_picture: rnd.bytes(rand(200) + 10)})
  end
  Album.create albums

  albums.each do |album|
    10.times do |num|
      tracks.append({album_id: album[:album_id],
                     track_number: num,
                     title: "#{adverbs.sample} #{verbs.sample}",
                     sample_rate: rand})
    end
  end
  Track.create tracks
end
