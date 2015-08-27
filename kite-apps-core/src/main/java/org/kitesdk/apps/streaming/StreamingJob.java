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

import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.JobParameters;

import java.util.Map;

/**
 * Marker interface for streaming jobs. Implementations of this interface
 * must provide a nullary constructor.
 */
public interface StreamingJob<T extends JobContext> {

  /**
   * Gets the parameters used to run the job.
   */
  JobParameters getParameters();

  /**
   * Runs the given streaming job.
   *
   * @param params map of job parameters by name
   * @param context the context in which the job is running.
   */
  public void runJob(Map<String,?> params, T context);
}
