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

first_names = %w[Emma Oliver Amelia Noah Charlotte Jacob Liam Ava Henry Sophia Mia Mason Evelyn Oliver Emily Amelia William Charlotte Noah Harper Elijah Avery James]
last_names = %w[Smith, Johnson, Williams, Jones, Brown, Davis, Miller, Wilson, Moore, Taylor, Anderson, Thomas, Jackson, White, Harris, Martin, Thompson, Garcia, Martinez, Robinson]

nouns = %w[
dog cat house car tree book computer phone television radio
sun moon star planet ocean sea river mountain valley forest
person girl boy woman man mother father sister brother family
]

adjectives = %w[
big small old young hot cold happy sad good bad
pretty ugly tall short fat thin smart stupid fast slow
brave cowardly kind cruel gentle rough friendly unfriendly
]

adverbs = %w[
quickly slowly carefully carelessly well badly suddenly gradually easily difficulty
loudly quietly happily sadly carefully carelessly correctly incorrectly interestingly boringly
]

verbs = %w[
run jump walk talk eat sleep drink think see hear
sing dance play read write draw paint cook clean wash
work study learn teach help share care love hate
]

venue_names = [
  "The Groove Room",
  "The Getaway",
  "The Sound Stage",
  "The Vibe Bar",
  "The Beat Club",
  "The Rhythm Lounge",
  "The Melody Room",
  "The Harmony Cafe",
  "The Treble Room",
  "The Bass Line",
  "The Jam Spot",
  "The Music Hall",
  "The Concert Club",
  "The Rock Bar",
  "The Jazz Club",
  "The Blues Bar",
  "The Country Club",
  "The Folk Cafe",
  "The Classical Music Hall",
  "The Opera House",
  "The Ballet Theatre",
  "The Symphony Hall",
  "The Choir Hall",
  "The Bandstand",
  "The Music Studio",
  "The Recording Studio",
  "The Rehearsal Space",
  "The Music School",
  "The Music Store",
  "The Music Museum",
  "The Music Library",
  "The Music Festival",
  "The Music Awards",
  "The Music Conference",
  "The Music Competition",
  "The Music Showcase",
  "The Music Tour",
  "The Music Concert",
  "The Music Recital",
  "The Music Performance",
  "The Music Jam Session",
  "The Music Open Mic Night"
]

rnd = Random.new

puts "Deleting existing data..."

Concert.delete_all
puts "Deleted concerts"

Venue.delete_all
puts "Deleted venues"

Album.delete_all
puts "Deleted albums"

Singer.delete_all
puts "Deleted singers"

puts "Creating random singers, albums, tracks, venues and concerts..."
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
  puts "Created #{singers.length} singers"

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
  puts "Created #{albums.length} albums"

  albums.each do |album|
    10.times do |num|
      tracks.append({album_id: album[:album_id],
                     track_number: num+1,
                     title: "#{adverbs.sample} #{verbs.sample}",
                     sample_rate: rand})
    end
  end
  Track.create tracks
  puts "Created #{tracks.length} tracks"

  10.times do
    venues.append({venue_id: SecureRandom.uuid,
                   name: venue_names.sample,
                   description: {
                     capacity: rand(20000),
                     type: "Arena",
                   }})
  end
  Venue.create venues
  puts "Created #{venues.length} venues"

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
  puts "Created #{concerts.length} concerts"
end
puts "Finished seeding data"
