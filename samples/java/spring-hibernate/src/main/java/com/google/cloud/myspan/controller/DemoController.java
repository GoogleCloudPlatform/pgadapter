package com.google.cloud.myspan.controller;

import com.google.cloud.myspan.entity.Albums;
import com.google.cloud.myspan.entity.Concerts;
import com.google.cloud.myspan.entity.MultiEntryIds;
import com.google.cloud.myspan.entity.Singers;
import com.google.cloud.myspan.entity.Tracks;
import com.google.cloud.myspan.service.SingerService;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
@Controller
public class DemoController {

  private static final Logger logger = LoggerFactory.getLogger(DemoController.class);

  @Autowired
  private SingerService singerService;

  @RequestMapping(value = "/execute-demo", method = RequestMethod.GET, produces = "application/json")
  public String executeDemo() {

    logger.info("Starting Demo!");
    singerService.getSingers();

    MultiEntryIds multiEntryIds = singerService.addTracksWithSingers(
        "Scientist", 42432, 4324, "India",
        "Mumbai", "Big city",
        "A Rush of Blood to the Head", "31231232131.3213",
        "Cold", "Play");

    Concerts concerts = singerService.getConcerts(multiEntryIds.getConcertId());

    Albums albums = singerService.getAlbum(multiEntryIds.getAlbumId());

    singerService.rollback();

    singerService.updateAlbumMarketingBudget(albums.getId(), "31232112.412414");

    logger.info("Demo executed successfully!");
    return "Demo executed successfully.";
  }

}
