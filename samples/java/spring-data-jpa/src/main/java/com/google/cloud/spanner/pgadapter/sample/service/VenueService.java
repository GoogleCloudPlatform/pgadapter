// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample.service;

import com.google.cloud.spanner.pgadapter.sample.model.Venue;
import com.google.cloud.spanner.pgadapter.sample.model.Venue.VenueDescription;
import com.google.cloud.spanner.pgadapter.sample.repository.VenueRepository;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.springframework.stereotype.Service;

@Service
public class VenueService {
  private final VenueRepository repository;

  private final RandomDataService randomDataService;

  public VenueService(VenueRepository repository, RandomDataService randomDataService) {
    this.repository = repository;
    this.randomDataService = randomDataService;
  }

  @Transactional
  public void deleteAllVenues() {
    repository.deleteAll();
  }

  @Transactional
  public List<Venue> generateRandomVenues(int count) {
    Random random = new Random();

    List<Venue> venues = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Venue venue = new Venue();
      venue.setName(randomDataService.getRandomVenueName());
      VenueDescription description = new VenueDescription();
      description.setCapacity(random.nextInt(100_000));
      description.setType(randomDataService.getRandomVenueType());
      description.setLocation(randomDataService.getRandomVenueLocation());
      venue.setDescription(description);
      venues.add(venue);
    }
    return repository.saveAll(venues);
  }
}
