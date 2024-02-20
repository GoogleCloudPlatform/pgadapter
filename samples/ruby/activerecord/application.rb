# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require 'active_record'
require 'io/console'
require_relative 'config/environment'
require_relative 'models/singer'
require_relative 'models/album'
require_relative 'models/concert'

# This sample application shows the basic features of
# ActiveRecord with Google Cloud Spanner PostgreSQL-dialect.
class Application
  def self.run
    # Query the current singers in the database.
    query_singers

    # Update a random singer.
    update_singer

    # Execute a query on the singers table.
    query_singers_by_name

    # List all singers and related venue information.
    query_concerts
  end

  def self.query_singers
    # Fetch all singers and albums from the database.
    # The database has been pre-filled by the `db/seeds.rb` script.
    puts ''
    puts 'Known singers and their albums:'
    puts ''
    Singer.all.each do |singer|
      puts "#{singer.first_name} #{singer.last_name} has albums:"
      singer.albums.each do |album|
        puts "   #{album.title} has tracks:"
        album.tracks.each do |track|
          puts "       #{track.track_number} #{track.title}"
        end
      end
    end
  end

  def self.update_singer
    # Select a random singer and update the name of this singer.
    puts ''
    singer = Singer.all.sample
    puts ''
    puts "Current name of singer #{singer.id} is '#{singer.first_name} #{singer.last_name}'"
    puts "Updating name to 'Dave Anderson'"
    singer.first_name = 'Dave'
    singer.last_name = 'Anderson'
    singer.save!
    singer.reload
    puts "New name of singer #{singer.id}: #{singer.first_name} #{singer.last_name}"
  end

  def self.query_singers_by_name
    # Select all singers whose last name start with 'A'.
    # This should include at least the singer that was updated
    # in the previous step, but probably also a number of other singers.
    puts ''
    puts "Getting all singers with a last name that starts with 'A'"

    last_name = Singer.arel_table['last_name']
    # The 'escape=nil, case_sensitive=true' is necessary to work around the lack of support
    # for ILIKE in Cloud Spanner.
    Singer.where(last_name.matches("A%", escape=nil, case_sensitive=true)).each do |s|
      puts "#{s.first_name} #{s.last_name}"
    end
  end

  # Lists all concerts and related venue information.
  def self.query_concerts
    puts ''
    puts 'Known concerts:'
    puts ''
    Concert.all.each do |concert|
      puts "#{concert.name} at #{concert.venue.name}"
      puts "  Start time: #{concert.start_time}"
      puts "  Venue description: #{concert.venue.description}"
      # Add a deliberate error to verify the test runner
      puts "  Venue description: #{concert.venue.foo}"
    end
  end

end

Application.run

