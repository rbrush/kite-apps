package org.kitesdk.apps.spi.oozie;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.scheduled.Schedule;

/**
 * Created by rb4106 on 4/27/15.
 */
public class TestApp extends AbstractApplication {

  @Override
  public void setup(Configuration conf) {

    Schedule schedule = new Schedule.Builder()
        .jobClass(TestJob.class)
        .withView("input.data", "dataset:hive:test/input", 1)
        .withView("output.data", "dataset:hive:test/output", 1)
        .hourly()
        .build();

    schedule(schedule);
  }
}
