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
import com.google.cloud.spanner.pgadapter.sample.model.Track;
import com.google.cloud.spanner.pgadapter.sample.repository.AlbumRepository;
import com.google.cloud.spanner.pgadapter.sample.repository.TrackRepository;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class TrackService {
  private final RandomDataService randomDataService;

  private final TrackRepository trackRepository;

  private final AlbumRepository albumRepository;

  public TrackService(
      RandomDataService randomDataService,
      TrackRepository trackRepository,
      AlbumRepository albumRepository) {
    this.randomDataService = randomDataService;
    this.trackRepository = trackRepository;
    this.albumRepository = albumRepository;
  }

  @Transactional
  public void generateRandomTracks(int numAlbums, int numTracksPerAlbum) {
    Random random = new Random();

    List<Album> albums = albumRepository.findAll(Pageable.ofSize(numAlbums)).toList();
    for (Album album : albums) {
      List<Track> tracks = new ArrayList<>(numTracksPerAlbum);
      for (int trackNumber = 1; trackNumber <= numTracksPerAlbum; trackNumber++) {
        Track track = Track.createNew(album, trackNumber);
        track.setTitle(randomDataService.getRandomTrackTitle());
        track.setSampleRate(random.nextDouble());
        tracks.add(track);
      }
      trackRepository.saveAll(tracks);
    }
  }
}
