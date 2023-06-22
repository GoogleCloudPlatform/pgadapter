package com.google.cloud.spanner.pgadapter.sample.service;

import com.google.cloud.spanner.pgadapter.sample.model.Album;
import com.google.cloud.spanner.pgadapter.sample.repository.AlbumRepository;
import com.google.cloud.spanner.pgadapter.sample.repository.SingerRepository;
import jakarta.transaction.Transactional;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.springframework.stereotype.Service;

@Service
public class AlbumService {
  private final RandomDataService randomDataService;
  
  private final AlbumRepository albumRepository;
  
  private final SingerRepository singerRepository;
  
  public AlbumService(RandomDataService randomDataService, AlbumRepository albumRepository, SingerRepository singerRepository) {
    this.randomDataService = randomDataService;
    this.albumRepository = albumRepository;
    this.singerRepository = singerRepository;
  }
  
  @Transactional
  public List<Album> generateRandomAlbums(int count) {
    Random random = new Random();
    
    List<Album> results = new ArrayList<>(count);
    for (int i=0; i<count; i++) {
      Album album = new Album();
      album.setTitle(randomDataService.getRandomAlbumTitle());
      byte[] picture = new byte[random.nextInt(400) + 100];
      random.nextBytes(picture);
      album.setCoverPicture(picture);
      album.setMarketingBudget(BigDecimal.valueOf(random.nextDouble()));
      album.setReleaseDate(LocalDate.of(random.nextInt(100) + 1923, random.nextInt(12) + 1, random.nextInt(28) + 1));
      results.add(albumRepository.save(album));
    }
    return results;
  }
}
