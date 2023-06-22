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

package com.google.cloud.spanner.pgadapter.sample;

import com.google.cloud.spanner.pgadapter.sample.model.Singer;
import com.google.cloud.spanner.pgadapter.sample.repository.SingerRepository;
import com.google.cloud.spanner.pgadapter.sample.service.SingerService;
import java.util.List;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SampleApplication implements CommandLineRunner {
  private static final Logger log = LoggerFactory.getLogger(SampleApplication.class);
  
  private static final PGAdapter pgAdapter = new PGAdapter();
  
  public static void main(String[] args) {
    SpringApplication.run(SampleApplication.class, args);
  }
  
  private final SingerService singerService;
  
  public SampleApplication(SingerService singerService) {
    this.singerService = singerService;
  }
  
  @Override
  public void run(String... args) throws Exception {
    // Generate some random data.
    List<Singer> singers = singerService.generateRandomSingers(10);
    log.info("Created 10 singers");
  }

  @PreDestroy
  public void onExit() {
    // Stop PGAdapter when the application is shut down.
    pgAdapter.stopPGAdapter();
  }

}
