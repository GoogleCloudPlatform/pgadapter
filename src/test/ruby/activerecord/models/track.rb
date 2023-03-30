# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

# Model for a Track on an Album.
class Track < ActiveRecord::Base
  self.primary_keys = :album_id, :track_number
  belongs_to :album
end
