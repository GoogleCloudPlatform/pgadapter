package com.google.cloud.spanner.pgadapter.sample.service;

import com.google.cloud.spanner.pgadapter.sample.model.Album;
import com.google.cloud.spanner.pgadapter.sample.model.Singer;
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

  public TrackService(RandomDataService randomDataService, TrackRepository trackRepository, AlbumRepository albumRepository) {
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
