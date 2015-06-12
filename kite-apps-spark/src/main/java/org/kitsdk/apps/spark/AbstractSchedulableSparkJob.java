package org.kitsdk.apps.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;

/**
 * Abstract base class for a scheduleable Spark job.
 */
public abstract class AbstractSchedulableSparkJob extends AbstractSchedulableJob {

  private JavaSparkContext context;

  public void setContext(JavaSparkContext context) {
    this.context = context;
  }

  public JavaSparkContext getContext() {
    return context;
  }
}
