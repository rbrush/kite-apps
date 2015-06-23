package org.kitesdk.apps.crunch;

import org.apache.crunch.util.DistCache;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;

import java.io.IOException;

/**
 * Abstract base class for Crunch-based jobs.
 */
public abstract class AbstractCrunchJob extends AbstractSchedulableJob {

  protected Pipeline getPipeline() {

    return new MRPipeline(AbstractCrunchJob.class, getName(), getConf());
  }
}
