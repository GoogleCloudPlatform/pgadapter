# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require 'securerandom'
require_relative '../config/environment.rb'
require_relative '../models/album'
require_relative '../models/concert'
require_relative '../models/singer'
require_relative '../models/track'
require_relative '../models/venue'

first_names = %w[Pete Alice John Ethel Trudy Naomi Wendy]
last_names = %w[Wendelson Allison Peterson Johnson Henderson Ericsson Aronson]

adjectives = %w[daily happy blue generous cooked bad open]
nouns = %w[windows potatoes bank street tree glass bottle]

adverbs = %w[swiftly surprisingly rather closely furiously politely currently unimpressively gracefully abnormally stealthily irritably]
verbs = %w[poke rescue tick long soothe obtain fancy prick suffer encourage love own]

rnd = Random.new

Concert.delete_all
Venue.delete_all
Album.delete_all
Singer.delete_all

Singer.transaction do
  singers = []
  albums = []
  tracks = []
  venues = []
  concerts = []
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

  10.times do
    venues.append({venue_id: SecureRandom.uuid,
                   name: "Venue #{rand(1000)}",
                   description: {
                     capacity: rand(20000),
                     type: "Arena",
                   }})
  end
  Venue.create venues

  50.times do
    singer = singers.sample
    date = Date.new(rand(100) + 1923, rand(12) + 1, rand(28) + 1)
    start_time = DateTime.new(date.year, date.month, date.day, rand(24), rand(60), 0)
    end_time = start_time + (rand(5) + 1).hours
    concerts.append({concert_id: SecureRandom.uuid,
                     name: "#{singer[:first_name]} #{singer[:last_name]} Live #{date}",
                     venue_id: venues.sample[:venue_id],
                     singer_id: singer[:singer_id],
                     start_time: start_time,
                     end_time: end_time})
  end
  Concert.create concerts
end
