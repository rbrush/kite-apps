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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.jobs.JobUtil;
import org.kitesdk.apps.streaming.StreamDescription;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Context in which a Kite job is running.
 */
public class JobContext {

  private final Job job;

  private final Configuration conf;

  private final Map<String,String> settings;

  /**
   * Gets the configuration settings for the job.
   */
  public Map<String,String> getSettings() {
    return settings;
  }


  /**
   * Returns true if the given setting is general and should be applied
   * to the input or outut.
   */
  private static boolean isGeneralSetting(String settingName) {

    return !settingName.startsWith("input") &&
        !settingName.startsWith("output");
  }

  /**
   * Gets the settings for a DataInput parameter
   * of the job, given the name used in DataInput annotation.
   */
  public Map<String,String> getInputSettings (String dataInputName) {

    Map<String,String> inputSettings = Maps.newHashMap();

    String prefix = "input." + dataInputName + ".";

    for (String setting: settings.keySet()) {

      if (setting.startsWith(prefix)) {

        inputSettings.put(setting.substring(prefix.length()), settings.get(setting));
      } else {
        if (isGeneralSetting(setting)) {
          inputSettings.put(setting, settings.get(setting));
        }
      }
    }

    return inputSettings;
  }

  /**
   * Gets the settings for a DataOutput parameter
   * of the job, given the name used in DataOutput annotation.
   */
  public Map<String,String> getOutputSettings (String dataOutputName) {

    Map<String,String> outputSettings = Maps.newHashMap();

    String prefix = "output." + dataOutputName + ".";

    for (String setting: settings.keySet()) {

      if (setting.startsWith(prefix)) {

        outputSettings.put(setting.substring(prefix.length()), settings.get(setting));
      } else {
        if (isGeneralSetting(setting)) {
          outputSettings.put(setting, settings.get(setting));
        }
      }
    }

    return outputSettings;
  }

  private static Map<String,String> getDefaultSettings(StreamDescription descrip) {

    descrip.getJobClass();

    Method runMethod = JobUtil.resolveRunMethod(descrip.getJobClass());

    Set<String> inputNames = JobUtil.getInputs(runMethod).keySet();

    Map<String,String> settings = Maps.newHashMap();

    for (StreamDescription.Stream stream: descrip.getStreams().values()) {

      for (Map.Entry<String,String> setting: stream.getProperties().entrySet()) {

        // Use the appropriate input or output configuration prefix for
        // the parameter.
        String prefix = inputNames.contains(stream.getName()) ? "input." : "output.";

        settings.put(prefix + stream.getName() + "." + setting.getKey(), setting.getValue());
      }
    }

    return settings;
  }

  /**
   * Gets the Hadoop configuration for the job.
   */
  public Configuration getHadoopConf() {
    return new Configuration(conf);
  }

  /**
   * Creates a context with the given settings and Hadoop configuration.
   */
  public JobContext(StreamDescription descrip, Job job, Map<String,String> settings, Configuration conf) {
    this.job = job;
    this.settings = toJobSettings(descrip, job.getName(), settings);
    this.conf = toJobHadoopConf(job.getName(), conf);
  }


  public JobContext(Job job, Map<String,String> settings, Configuration conf) {
    this.job = job;
    this.settings = toJobSettings(job.getName(), settings);
    this.conf = toJobHadoopConf(job.getName(), conf);
  }

  private static Map<String,String> toJobSettings(String jobName, Map<String,String> settings) {

    // TODO: implement.
    return settings;
  }

  private static Map<String,String> toJobSettings(StreamDescription description, String jobName,   Map<String,String> appSettings) {

    Map<String,String> jobSettings = getDefaultSettings(description);

    for (Map.Entry<String,String> entry: appSettings.entrySet()) {
      // TODO: get qualified settings for the job.
      jobSettings.put(entry.getKey(), entry.getValue());
    }

    return jobSettings;
  }

  private static Configuration toJobHadoopConf(String jobName, Configuration conf) {

    // TODO: implement.
    return conf;
  }
}
