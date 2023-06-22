package com.google.cloud.spanner.pgadapter.sample.service;

import com.google.cloud.spanner.pgadapter.sample.model.Singer;
import com.google.cloud.spanner.pgadapter.sample.repository.SingerRepository;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class SingerService {
  private final RandomDataService randomDataService;
  
  private final SingerRepository repository;
  
  public SingerService(RandomDataService randomDataService, SingerRepository repository) {
    this.randomDataService = randomDataService;
    this.repository = repository;
  }
  
  @Transactional
  public List<Singer> generateRandomSingers(int count) {
    List<Singer> results = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Singer singer = new Singer();
      singer.setFirstName(randomDataService.getRandomFirstName());
      singer.setLastName(randomDataService.getRandomLastName());
      results.add(repository.save(singer));
    }
    return results;
  }
}
