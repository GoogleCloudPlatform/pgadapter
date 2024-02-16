// Copyright 2024 Google LLC
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

import com.google.cloud.spanner.pgadapter.sample.model.Concert;
import com.google.cloud.spanner.pgadapter.sample.model.TicketSale;
import com.google.cloud.spanner.pgadapter.sample.repository.ConcertRepository;
import com.google.cloud.spanner.pgadapter.sample.repository.TicketSaleRepository;
import jakarta.transaction.Transactional;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class TicketSaleService {
  private final TicketSaleRepository repository;

  private final ConcertRepository concertRepository;

  private final RandomDataService randomDataService;

  public TicketSaleService(
      TicketSaleRepository repository,
      ConcertRepository concertRepository,
      RandomDataService randomDataService) {
    this.repository = repository;
    this.concertRepository = concertRepository;
    this.randomDataService = randomDataService;
  }

  @Transactional
  public void deleteAllTicketSales() {
    repository.deleteAll();
  }

  @Transactional
  public List<TicketSale> generateRandomTicketSales(int count) {
    Random random = new Random();

    List<Concert> concerts = concertRepository.findAll(Pageable.ofSize(10)).toList();
    List<TicketSale> ticketSales = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      TicketSale ticketSale = new TicketSale();
      ticketSale.setConcert(concerts.get(random.nextInt(concerts.size())));
      ticketSale.setCustomerName(
          randomDataService.getRandomFirstName() + " " + randomDataService.getRandomLastName());
      ticketSale.setPrice(
          new BigDecimal(random.nextInt(30000))
              .setScale(2, RoundingMode.HALF_UP)
              .divide(new BigDecimal(100), RoundingMode.HALF_UP));
      ticketSale.setSeats(
          IntStream.range(0, random.nextInt(10) + 1)
              .mapToObj(Integer::toString)
              .collect(Collectors.toList()));
      ticketSales.add(ticketSale);
    }
    return repository.saveAll(ticketSales);
  }

  public List<TicketSale> all() {
    return repository.findAll();
  }
}
