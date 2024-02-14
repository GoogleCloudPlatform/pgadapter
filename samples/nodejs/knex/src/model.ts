// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as buffer from "buffer";

export interface Singer {
  id: string;
  first_name: string;
  last_name: string;
  full_name: string;
  active: boolean;
  created_at: Date;
  updated_at: Date;
}

export interface Album {
  id: string;
  title: string;
  marketing_budget: number;
  release_date: Date;
  cover_picture: Buffer;
  singer_id: string;
  created_at: Date;
  updated_at: Date;
}

export interface Track {
  id: string; // Table "tracks" is interleaved in "albums", so this "id" is equal to "albums"."id".
  track_number: number;
  title: string;
  sample_rate: number;
  created_at: Date;
  updated_at: Date;
}

export interface Venue {
  id: string;
  name: string;
  description: string;
  created_at: Date;
  updated_at: Date;
}

export interface Concert {
  id: string;
  venue_id: string;
  singer_id: string;
  name: string;
  start_time: Date;
  end_time: Date;
  created_at: Date;
  updated_at: Date;
}

export interface TicketSale {
  id: number;
  concert_id: string;
  customer_name: string;
  price: number;
  seats: string[];
  created_at: Date;
  updated_at: Date;
}
