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

package com.google.cloud.spanner.hibernate;

import com.google.common.collect.Range;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.hibernate.boot.model.relational.AuxiliaryDatabaseObject;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.enhanced.SequenceStructure;

/** This class generates a bit-reversed sequence for a Cloud Spanner PostgreSQL database. */
public class BitReversedSequenceStructure extends SequenceStructure {
  private final String contributor;
  private final QualifiedName qualifiedSequenceName;
  private final int initialValue;
  private final List<Range<Long>> excludedRanges;

  public BitReversedSequenceStructure(
      JdbcEnvironment jdbcEnvironment,
      String contributor,
      QualifiedName qualifiedSequenceName,
      int initialValue,
      int incrementSize,
      List<Range<Long>> excludedRanges,
      Class numberType) {
    super(
        jdbcEnvironment,
        contributor,
        qualifiedSequenceName,
        initialValue,
        incrementSize,
        numberType);
    this.contributor = contributor;
    this.qualifiedSequenceName = qualifiedSequenceName;
    this.initialValue = initialValue;
    this.excludedRanges = excludedRanges;
  }

  static class BitReversedSequenceAuxiliaryDatabaseObject implements AuxiliaryDatabaseObject {
    private final Sequence sequence;

    private final List<Range<Long>> excludeRanges;

    BitReversedSequenceAuxiliaryDatabaseObject(Sequence sequence, List<Range<Long>> excludeRanges) {
      this.sequence = sequence;
      this.excludeRanges = excludeRanges;
    }

    @Override
    public boolean appliesToDialect(Dialect dialect) {
      return true;
    }

    @Override
    public boolean beforeTablesOnCreation() {
      return true;
    }

    @Override
    public String[] sqlCreateStrings(SqlStringGenerationContext context) {
      return new String[] {
          context
              .getDialect()
              .getSequenceSupport()
              .getCreateSequenceString(context.format(sequence.getName()))
              + " bit_reversed_positive"
              + buildSkipRangeOptions(excludeRanges)
              + buildStartCounterOption(sequence.getInitialValue())
      };
    }

    @Override
    public String[] sqlDropStrings(SqlStringGenerationContext context) {
      return context
          .getDialect()
          .getSequenceSupport()
          .getDropSequenceStrings(context.format(sequence.getName()));
    }

    @Override
    public String getExportIdentifier() {
      return sequence.getExportIdentifier();
    }
  }

  @Override
  protected void buildSequence(Database database) {
    Sequence sequence;
    Optional<AuxiliaryDatabaseObject> existing =
        database.getAuxiliaryDatabaseObjects().stream()
            .filter(aux -> aux.getExportIdentifier().equals(qualifiedSequenceName.render()))
            .findAny();
    if (existing.isPresent()) {
      sequence = ((BitReversedSequenceAuxiliaryDatabaseObject) existing.get()).sequence;
    } else {
      final Namespace namespace =
          database.locateNamespace(
              qualifiedSequenceName.getCatalogName(), qualifiedSequenceName.getSchemaName());
      // Create a sequence and then remove it again from the namespace, as we'll be adding it as an
      // auxiliary database object.
      sequence =
          namespace.createSequence(
              qualifiedSequenceName.getObjectName(),
              (physicalName) ->
                  new Sequence(
                      contributor,
                      namespace.getPhysicalName().getCatalog(),
                      namespace.getPhysicalName().getSchema(),
                      physicalName,
                      initialValue,
                      1));
      database.addAuxiliaryDatabaseObject(
          new BitReversedSequenceAuxiliaryDatabaseObject(sequence, excludedRanges));
      Iterator<Sequence> iterator = namespace.getSequences().iterator();
      while (iterator.hasNext()) {
        if (iterator.next() == sequence) {
          iterator.remove();
          break;
        }
      }
    }
    this.physicalSequenceName = sequence.getName();
  }

  private static String buildSkipRangeOptions(List<Range<Long>> excludeRanges) {
    if (excludeRanges.isEmpty()) {
      return "";
    }
    return String.format(
        " skip range %d %d", getMinSkipRange(excludeRanges), getMaxSkipRange(excludeRanges));
  }

  private static long getMinSkipRange(List<Range<Long>> excludeRanges) {
    return excludeRanges.stream().map(Range::lowerEndpoint).min(Long::compare).orElse(0L);
  }

  private static long getMaxSkipRange(List<Range<Long>> excludeRanges) {
    return excludeRanges.stream()
        .map(Range::upperEndpoint)
        .max(Long::compare)
        .orElse(Long.MAX_VALUE);
  }

  private static String buildStartCounterOption(int initialValue) {
    return initialValue == 1 ? "" : String.format(" start counter with %d", initialValue);
  }
}
