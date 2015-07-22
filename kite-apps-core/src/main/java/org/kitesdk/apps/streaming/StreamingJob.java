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
package org.kitesdk.apps.streaming;


import org.apache.hadoop.conf.Configurable;
import org.kitesdk.apps.JobContext;

/**
 * Interface for streaming jobs. Implementations of this interface
 * must provide a nullary constructor.
 */
public interface StreamingJob<T extends JobContext>  {

  /**
   * The name of the streaming job, which may be the class name.
   * This name will be used in job-specific configuration files and
   * be visible in system management tooling.
   */
  String getName();

  /**
   * Sets the context for the job. This is called before the job is run.
   */
  void setJobContext(T jobContext);

  /**
   * Gets the context for the job.
   */
  T getJobContext();
}
