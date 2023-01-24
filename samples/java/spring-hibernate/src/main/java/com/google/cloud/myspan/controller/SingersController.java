package com.google.cloud.myspan.controller;

import com.google.cloud.myspan.entity.Albums;
import com.google.cloud.myspan.entity.Concerts;
import com.google.cloud.myspan.entity.MultiEntryIds;
import com.google.cloud.myspan.entity.Singers;
import com.google.cloud.myspan.entity.Tracks;
import com.google.cloud.myspan.entity.TracksId;
import com.google.cloud.myspan.service.SingerService;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
@Controller
public class SingersController {

  @Autowired
  private SingerService singerService;

  @RequestMapping(value = "/all-singers", method = RequestMethod.GET, produces = "application/json")
  public List<Singers> allSingers() {
    return singerService.getSingers();
  }

  @RequestMapping(value = "/multi-table-entry", method = RequestMethod.GET, produces = "application/json")
  public MultiEntryIds multiTableEntry(String trackName, int trackNumber, int sampleRate,
      String concertName,
      String venueName, String venueDescription, String albumName, String marketingBudget,
      String singerFirstName, String singerLastName) {
    return singerService.addTracksWithSingers(trackName, trackNumber, sampleRate, concertName,
        venueName,
        venueDescription, albumName, marketingBudget, singerFirstName, singerLastName);
  }

  @RequestMapping(value = "/fetch-concerts", method = RequestMethod.GET, produces = "application/json")
  public Concerts fetchConcerts(String concertId) {
    Concerts concerts = singerService.getConcerts(UUID.fromString(concertId));
    return concerts;
  }

  @RequestMapping(value = "/album-tracks", method = RequestMethod.GET, produces = "application/json")
  public List<Tracks> fetchAlbumTracks(String albumId) {
    return singerService.getTracksForAlbums(UUID.fromString(albumId));
  }

  @RequestMapping(value = "/get-album", method = RequestMethod.GET, produces = "application/json")
  public Albums getAlbum(String albumId) {
    return singerService.getAlbum(UUID.fromString(albumId));
  }

  @RequestMapping(value = "/get-singers", method = RequestMethod.GET, produces = "application/json")
  public Singers getSingers(String singerId) {
    return singerService.getSingers(UUID.fromString(singerId));
  }

  @RequestMapping(value = "/update-album-marketing-budget", method = RequestMethod.GET, produces = "application/json")
  public Albums fetchAlbumTracks(String albumId, String marketingBudget) {
    return singerService.updateAlbumMarketingBudget(UUID.fromString(albumId), marketingBudget);
  }

  @RequestMapping(value = "/add-singer", method = RequestMethod.GET, produces = "application/json")
  public String addSingers(@RequestParam("firstName") String firstName,
      @RequestParam("lastName") String lastName) {
    return "Singer with Id: " + singerService.addSinger(firstName, lastName) + " added.";
  }

  @RequestMapping(value = "/rollback-test", method = RequestMethod.GET, produces = "application/json")
  public String rollbackTest() {

    return "Transaction successfully rolled back. Created singers Id: " + singerService.rollback();
  }

}
