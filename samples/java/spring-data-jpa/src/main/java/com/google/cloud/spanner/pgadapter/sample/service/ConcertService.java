package com.google.cloud.spanner.pgadapter.sample.service;

import com.google.cloud.spanner.pgadapter.sample.model.Concert;
import com.google.cloud.spanner.pgadapter.sample.model.Singer;
import com.google.cloud.spanner.pgadapter.sample.model.Venue;
import com.google.cloud.spanner.pgadapter.sample.repository.ConcertRepository;
import com.google.cloud.spanner.pgadapter.sample.repository.SingerRepository;
import com.google.cloud.spanner.pgadapter.sample.repository.VenueRepository;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class ConcertService {
  private final ConcertRepository repository;

  private final SingerRepository singerRepository;

  private final VenueRepository venueRepository;

  private final RandomDataService randomDataService;

  public ConcertService(
      ConcertRepository repository,
      SingerRepository singerRepository,
      VenueRepository venueRepository,
      RandomDataService randomDataService) {
    this.repository = repository;
    this.singerRepository = singerRepository;
    this.venueRepository = venueRepository;
    this.randomDataService = randomDataService;
  }

  public List<Concert> generateRandomConcerts(int count) {
    Random random = new Random();

    List<Singer> singers = singerRepository.findAll(Pageable.ofSize(20)).toList();
    List<Venue> venues = venueRepository.findAll();
    List<Concert> concerts = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Concert concert = new Concert();
      concert.setName(randomDataService.getRandomConcertName());
      concert.setSinger(singers.get(random.nextInt(singers.size())));
      concert.setVenue(venues.get(random.nextInt(venues.size())));
      concert.setStartTime(
          OffsetDateTime.of(
              random.nextInt(30) + 1995,
              random.nextInt(12) + 1,
              random.nextInt(28) + 1,
              random.nextInt(24),
              random.nextBoolean() ? 0 : 30,
              0,
              0,
              ZoneOffset.ofHours(random.nextInt(24) - 12)));
      concert.setEndTime(concert.getStartTime().plus(Duration.ofHours(random.nextInt(6) + 1)));
      concerts.add(concert);
    }
    return repository.saveAll(concerts);
  }
}
