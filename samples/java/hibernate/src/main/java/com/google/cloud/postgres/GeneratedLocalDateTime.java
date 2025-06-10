package com.google.cloud.postgres;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.hibernate.annotations.ValueGenerationType;
import org.hibernate.generator.EventType;

@ValueGenerationType(generatedBy = CurrentLocalDateTimeGenerator.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Inherited
public @interface GeneratedLocalDateTime {
  EventType[] eventTypes();
}
