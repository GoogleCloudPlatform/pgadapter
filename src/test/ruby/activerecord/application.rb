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
  end

  def self.query_singers
    # Fetch all singers and albums from the database.
    # The database has been pre-filled by the `db/seeds.rb` script.
    puts 'Known singers and their albums:'
    puts ''
    Singer.all.each do |singer|
      puts "#{singer.first_name} #{singer.last_name}"
      singer.albums.each do |album|
        puts "   #{album.title}"
      end
    end
  end

  def self.update_singer
    # Select a random singer and update the name of this singer.
    puts ''
    singer = Singer.all.sample
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
    # Singer.where("#{last_name.name} LIKE :prefix", prefix: "A%").each do |s|
    Singer.where(last_name.matches("A%", escape=nil, case_sensitive=true)).each do |s|
      puts "#{s.first_name} #{s.last_name}"
    end
  end
end

Application.run

