/**
 * Copyright 2015 Cerner Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.apps.spi.jobs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.scheduled.SchedulableJob;

import java.util.List;
import java.util.ServiceLoader;

/**
 * Source of {@link SchedulableJobManager} implementations.
 */
public abstract class JobManagers {

  private static final List<JobManagerFactory> FACTORIES = Lists.newArrayList();

  static class DefaultJobManagerFactory implements JobManagerFactory {

    @Override
    public boolean supports(Class jobClass) {
      return SchedulableJob.class.isAssignableFrom(jobClass);
    }

    @Override
    public SchedulableJobManager createManager(Class jobClass, Configuration conf) {
      return JavaActionJobManager.create(jobClass, conf);
    }
  }

  private static final JobManagerFactory DEFAULT_INSTANCE = new DefaultJobManagerFactory();

  public static SchedulableJobManager create(Class<? extends SchedulableJob> jobClass, Configuration conf) {

    for (JobManagerFactory factory: FACTORIES) {

      if (factory.supports(jobClass))
        return factory.createManager(jobClass, conf);
    }

    if (!DEFAULT_INSTANCE.supports(jobClass)) {
      throw new IllegalArgumentException("Job class " + jobClass + " not supported by any scheduled job manager.");
    }

    return DEFAULT_INSTANCE.createManager(jobClass, conf);
  }

  static {
    ServiceLoader<JobManagerFactory> impls = ServiceLoader.load(JobManagerFactory.class);

    for (JobManagerFactory factory: impls) {
      FACTORIES.add(factory);
    }
  }
}
