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
