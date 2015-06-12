package org.kitsdk.apps.spark.spi;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Default spark context for the application.
 */
public class DefaultSparkContext {

  private static volatile JavaSparkContext context;

  public static void setContext(JavaSparkContext context) {
    DefaultSparkContext.context = context;
  }

  public static JavaSparkContext getContext() {
    return context;
  }
}
