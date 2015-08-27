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
package org.kitesdk.apps;

/**
 * Base interface for Kite jobs. Actual jobs must implement either
 * {@link org.kitesdk.apps.scheduled.SchedulableJob} or
 * {@link org.kitesdk.apps.streaming.StreamingJob}.
 */
public interface Job<T extends JobContext> {

  /**
   * Sets the context for the job. This is called before the job is run.
   */
  void setJobContext(T jobContext);

  /**
   * Gets the context for the job.
   */
  T getJobContext();
}
