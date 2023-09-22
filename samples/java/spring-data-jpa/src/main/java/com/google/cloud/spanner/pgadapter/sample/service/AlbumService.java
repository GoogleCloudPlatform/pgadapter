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

import com.google.cloud.spanner.pgadapter.sample.model.Album;
import com.google.cloud.spanner.pgadapter.sample.model.Singer;
import com.google.cloud.spanner.pgadapter.sample.repository.AlbumRepository;
import com.google.cloud.spanner.pgadapter.sample.repository.SingerRepository;
import jakarta.transaction.Transactional;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class AlbumService {
  private final RandomDataService randomDataService;

  private final AlbumRepository albumRepository;

  private final SingerRepository singerRepository;

  public AlbumService(
      RandomDataService randomDataService,
      AlbumRepository albumRepository,
      SingerRepository singerRepository) {
    this.randomDataService = randomDataService;
    this.albumRepository = albumRepository;
    this.singerRepository = singerRepository;
  }

  @Transactional
  public void deleteAllAlbums() {
    albumRepository.deleteAll();
  }

  @Transactional
  public List<Album> generateRandomAlbums(int count) {
    Random random = new Random();

    // Get the first 20 singers and link the albums to those.
    List<Singer> singers = singerRepository.findAll(Pageable.ofSize(20)).toList();
    List<Album> albums = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Album album = new Album();
      album.setTitle(randomDataService.getRandomAlbumTitle());
      byte[] picture = new byte[random.nextInt(400) + 100];
      random.nextBytes(picture);
      album.setCoverPicture(picture);
      album.setMarketingBudget(BigDecimal.valueOf(random.nextDouble()));
      album.setReleaseDate(
          LocalDate.of(random.nextInt(100) + 1923, random.nextInt(12) + 1, random.nextInt(28) + 1));
      album.setSinger(singers.get(random.nextInt(singers.size())));
      albums.add(album);
    }
    return albumRepository.saveAll(albums);
  }
}
