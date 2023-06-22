package com.google.cloud.spanner.pgadapter.sample.service;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.springframework.stereotype.Service;

@Service
public class RandomDataService {
  private static final List<String> FIRST_NAMES = Arrays.asList(
      "Aarav",
      "Gunnar",
      "Joaquin",
      "Luciano",
      "Mateo",
      "Roman",
      "Santiago",
      "Soren",
      "Aurelio",
      "Laszlo",
      "Elio",
      "Wolf",
      "Amir",
      "Aditya",
      "Ayaan",
      "Dhruv",
      "Harsh",
      "Krishna",
      "Rohan",
      "Vihaan"
  );

  private static final List<String> LAST_NAMES = Arrays.asList(
      "Abreu",
      "Chen",
      "Da Silva",
      "Elliott",
      "Fernandez",
      "Garcia",
      "Gonzalez",
      "Hernandez",
      "Ibrahim",
      "James",
      "Kim",
      "Lopez",
      "Martinez",
      "Nguyen",
      "O'Brien",
      "Perez",
      "Ramos",
      "Smith",
      "Thomas",
      "Wang"
  );
  
  private static final List<String> NOUNS = Arrays.asList(
      "ocean",
      "river",
      "island",
      "mountain",
      "lake",
      "forest",
      "desert",
      "plain",
      "valley",
      "hill",
      "cloud",
      "star",
      "planet",
      "moon",
      "sun",
      "galaxy",
      "universe",
      "time",
      "space",
      "love"
  );

  private static final List<String> ADJECTIVES = Arrays.asList(
      "wondrous",
      "whimsical",
      "ethereal",
      "mystical",
      "dreamy",
      "serene",
      "tranquil",
      "peaceful",
      "joyful",
      "ecstatic",
      "euphoric",
      "sublime",
      "glorious",
      "majestic",
      "grandiose",
      "spectacular",
      "awe-inspiring",
      "mesmerizing",
      "enchanting",
      "captivating"
  );

  private final Random random = new Random();

  public String getRandomFirstName() {
    return FIRST_NAMES.get(random.nextInt(FIRST_NAMES.size()));
  }

  public String getRandomLastName() {
    return LAST_NAMES.get(random.nextInt(LAST_NAMES.size()));
  }
  
  public String getRandomAlbumTitle() {
    return ADJECTIVES.get(random.nextInt(ADJECTIVES.size())) + " " + NOUNS.get(random.nextInt(NOUNS.size()));
  }
  
  
}
