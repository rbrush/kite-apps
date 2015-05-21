package org.kitesdk.apps.scheduled;

import org.apache.avro.generic.GenericData;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for use on job parameters to indicate an input dataset.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface DataIn  {

  /**
   * The name of the input to be used in a {@link Schedule}
   * or other tooling.
   */
  String name();

  /**
   * The type of the input, defaulting to a generic record.
   */
  Class type() default GenericData.Record.class;
}
