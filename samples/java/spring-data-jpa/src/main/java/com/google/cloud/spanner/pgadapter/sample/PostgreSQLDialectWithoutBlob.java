package com.google.cloud.spanner.pgadapter.sample;

import java.sql.Types;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.descriptor.jdbc.BlobJdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;

/**
 * PostgreSQL Hibernate dialect that uses byte[] instead of Blob, even if the column is annotated
 * with @{@link jakarta.persistence.Lob}.
 */
public class PostgreSQLDialectWithoutBlob extends PostgreSQLDialect {

  @Override
  protected void contributePostgreSQLTypes(TypeContributions typeContributions,
      ServiceRegistry serviceRegistry) {
    super.contributePostgreSQLTypes(typeContributions, serviceRegistry);
    final JdbcTypeRegistry jdbcTypeRegistry = typeContributions.getTypeConfiguration()
        .getJdbcTypeRegistry();
    // Replace the default BLOB handling of Hibernate with one that just uses simple byte[].
    jdbcTypeRegistry.addDescriptor(Types.BLOB, BlobJdbcType.PRIMITIVE_ARRAY_BINDING);
  }
}
