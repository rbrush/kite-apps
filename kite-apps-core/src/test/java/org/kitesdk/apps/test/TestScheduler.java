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
package org.kitesdk.apps.test;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.Application;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;

import java.util.Map;

/**
 * Scheduling tool for testing Kite applications.
 */
public class TestScheduler {

  private final Application app;

  private final Configuration conf;

  TestScheduler(Application app, Configuration conf) {
    this.app = app;
    this.conf = conf;
  }

  public static TestScheduler load(Class<? extends Application> appClass,
                                     Configuration conf) {

    Application app;

    try {
      app = appClass.newInstance();

    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }

    // Set up the application.
    app.setup(conf);

    return new TestScheduler(app, conf);
  }

  /**
   * Converts the template into a View URI based on the given nominal time.
   */
  private static String resolveTemplate(String uriTemplate, Instant nominalTime) {

    DateTime time = nominalTime.toDateTime(DateTimeZone.UTC);

    return uriTemplate.replace("${YEAR}", Integer.toString(time.getYear()))
        .replace("${MONTH}", Integer.toString(time.getMonthOfYear()))
        .replace("${DAY}", Integer.toString(time.getDayOfMonth()))
        .replace("${HOUR}", Integer.toString(time.getHourOfDay()))
        .replace("${MINUTE}", Integer.toString(time.getMinuteOfHour()));
  }

  /**
   * Runs all scheduled jobs in the application using the given
   * nominal time.
   */
  public void runScheduledJobs(Instant nominalTime) {

    Map<String,View> views = Maps.newHashMap();

    for (Schedule schedule: app.getSchedules()) {

      for (Schedule.ViewTemplate input: schedule.getViewTemplates().values()) {

        String uri = resolveTemplate(input.getUriTemplate(), nominalTime);

        View view = Datasets.load(uri, input.getInputType());

        views.put(input.getName(), view);
      }
    }

    runScheduledJobs(nominalTime, views);
  }

  public void runScheduledJobs(Instant nominalTime, Object... argToViews) {

    Map<String,View> views = Maps.newHashMap();

    if (argToViews.length % 2 != 0) {
      throw new IllegalArgumentException("argToViews parameters must be of even length, containing" +
          "arg, view tuples.");
    }

    for (int i = 0; i < argToViews.length; i += 2) {

      views.put((String) argToViews[i], (View) argToViews[i + 1]);
    }

    runScheduledJobs(nominalTime, views);
  }

  public void runScheduledJobs(Instant nominalTime, Map<String,View> views) {

    for (Schedule schedule: app.getSchedules()) {

      SchedulableJobManager manager = JobManagers.createSchedulable(schedule.getJobClass(), conf);

      manager.run(nominalTime, views);
    }
  }
}
