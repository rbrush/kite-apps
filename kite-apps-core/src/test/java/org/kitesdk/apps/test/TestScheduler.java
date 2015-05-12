package org.kitesdk.apps.test;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.Application;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.ScheduledJobRunner;
import org.kitesdk.data.View;

import java.util.Collections;
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
   * Runs all scheduled jobs in the application using the given
   * nominal time.
   */
  public void runScheduledJobs(Instant nominalTime) {

    runScheduledJobs(nominalTime, Collections.<String,View>emptyMap());
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

      ScheduledJobRunner runner = ScheduledJobRunner.create(schedule.getJobClass(), conf);

      runner.run(nominalTime, views);
    }
  }
}
