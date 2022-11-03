package com.google.cloud.postgres;

import java.time.LocalDateTime;
import org.hibernate.Session;
import org.hibernate.tuple.ValueGenerator;

public class CurrentLocalDateTimeGenerator implements ValueGenerator<LocalDateTime> {

  @Override
  public LocalDateTime generateValue(Session session, Object entity) {
    return LocalDateTime.now();
  }
}
