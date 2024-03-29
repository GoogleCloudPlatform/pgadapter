# frozen_string_literal: true

# Copyright 2023 Google LLC
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

require_relative 'concert'

# Model for a Venue entity.
class Venue < ActiveRecord::Base
  has_many :concerts
end
