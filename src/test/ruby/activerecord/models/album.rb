# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

# Model for an Album of a Singer.
class Album < ActiveRecord::Base
  belongs_to :singer
  has_many :tracks
end
