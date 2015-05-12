package org.kitesdk.apps;


import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.scheduled.Schedule;

import java.util.List;

/**
 * Created by rb4106 on 3/30/15.
 */
public interface Application {

  public void setup(Configuration conf);

  public List<Schedule> getSchedules();
}
