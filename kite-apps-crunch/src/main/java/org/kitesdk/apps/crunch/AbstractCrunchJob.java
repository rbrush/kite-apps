package org.kitesdk.apps.crunch;

import org.kitesdk.apps.scheduled.AbstractScheduledJob;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;

/**
 * Abstract base class for Crunch-based jobs.
 */
public class AbstractCrunchJob extends AbstractScheduledJob {

  protected Pipeline getPipeline() {
    return new MRPipeline(AbstractCrunchJob.class, getConf());
  }
}
