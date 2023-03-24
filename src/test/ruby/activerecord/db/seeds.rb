# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require_relative '../config/environment.rb'
require_relative '../models/singer'
require_relative '../models/album'

first_names = %w[Pete Alice John Ethel Trudy Naomi Wendy]
last_names = %w[Wendelson Allison Peterson Johnson Henderson Ericsson Aronson]

adjectives = %w[daily happy blue generous cooked bad open]
nouns = %w[windows potatoes bank street tree glass bottle]

Album.delete_all
Singer.delete_all

10.times do
  Singer.create first_name: first_names.sample, last_name: last_names.sample
end

30.times do
  singer_id = Singer.all.sample.id
  Album.create title: "#{adjectives.sample} #{nouns.sample}", singer_id: singer_id
end
