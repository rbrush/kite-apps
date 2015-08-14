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

import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.streaming.StreamDescription;

import java.util.List;

/**
 * Kite applications are defined by implementations of this interface.
 *
 * <p>
 * Applications must implement a {@link #setup} method, in which they
 * can perform arbitrary installation steps for the application. This can
 * include creating Kite datasets, importing existing data, or performing
 * any initialization needed for the application.
 * </p>
 * <p>
 * Applications typically schedule one or more jobs to do work based
 * on either a time-based schedule or the arrival of data. See the
 * {@link org.kitesdk.apps.scheduled.Schedule} and
 * {@link org.kitesdk.apps.scheduled.SchedulableJob} interfaces
 * for details.
 * </p>
 */
public interface Application {

  /**
   * Sets up the application.  This can perform arbitrary
   * operations such as installing datasets, importing data,
   * or configuring scheduled jobs.
   *
   * @param context Context and configuration for the application's environment.
   */
  public void setup(AppContext context);

  /**
   * Gets the list of schedules for jobs that are launched
   * by this application.
   *
   * @return a list of schedules
   */
  public List<Schedule> getSchedules();

  /**
   * Gets the list of stream descriptions defining stream-based
   * jobs.
   *
   * @return a list of stream descriptions.
   */
  public List<StreamDescription> getStreamDescriptions();
}
