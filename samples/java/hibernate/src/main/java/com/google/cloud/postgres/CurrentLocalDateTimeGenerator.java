// Copyright 2022 Google LLC
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

package com.google.cloud.postgres;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.EnumSet;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.EventType;

public class CurrentLocalDateTimeGenerator implements BeforeExecutionGenerator {
  private final EnumSet<EventType> eventTypes;

  public CurrentLocalDateTimeGenerator(GeneratedLocalDateTime annotation) {
    eventTypes = EnumSet.copyOf(Arrays.asList(annotation.eventTypes()));
  }

  @Override
  public Object generate(
      SharedSessionContractImplementor session,
      Object owner,
      Object currentValue,
      EventType eventType) {
    return LocalDateTime.now();
  }

  @Override
  public EnumSet<EventType> getEventTypes() {
    return this.eventTypes;
  }
}
