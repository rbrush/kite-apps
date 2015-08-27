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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.apps.streaming.MockStreamingJob;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.apps.test.apps.ScheduledInputOutputJob;

import java.util.Collections;
import java.util.Map;

public class JobContextTest {

  private static final String JOB_NAME = "test-job-name";

  private AppContext context(String... settings) {

    Map<String,String> settingsMap = Maps.newHashMap();

    if (settings.length % 2 != 0)
      throw new IllegalArgumentException("Must have an even number of key/valeus in settings.");

    for (int i = 0; i < settings.length; i += 2) {

      settingsMap.put(settings[i], settings[i+1]);
    }

    return new AppContext(settingsMap, new Configuration());
  }

  private JobContext jobContext(AppContext context) {

    return new JobContext(JOB_NAME, context.getSettings(), context.getHadoopConf());
  }

  @Test
  public void testJobSpecificSettings() {

    AppContext context = context("my.app.setting", "my.app.value", "kite.job." + JOB_NAME + ".my.job.setting", "my.job.value");

    Assert.assertEquals(ImmutableMap.of("my.app.setting", "my.app.value",
        "kite.job." + JOB_NAME + ".my.job.setting", "my.job.value"),
        context.getSettings());

    Assert.assertEquals(
        ImmutableMap.of("my.app.setting", "my.app.value",
            "my.job.setting", "my.job.value"),
        jobContext(context).getSettings());
  }

  @Test
  public void testJobOverridesAppSetting() {

    AppContext context = context("my.app.setting", "my.app.value",
        "my.default.setting", "my.default.value",
        "kite.job." + JOB_NAME + ".my.default.setting", "my.updated.value");

    Assert.assertEquals(
        ImmutableMap.of("my.app.setting", "my.app.value",
            "my.default.setting", "my.updated.value"),
        jobContext(context).getSettings());
  }

  @Test
  public void testInputOutputSettings() {

    AppContext context = context("kite.job." + JOB_NAME + ".input.source-users.my.input.setting", "my.input.value",
        "kite.job." + JOB_NAME + ".output.target-users.my.output.setting", "my.output.value",
        "some.other.setting", "some.other.value");

    JobContext jobContext = jobContext(context);

    Assert.assertEquals(
        ImmutableMap.of("my.input.setting", "my.input.value",
            "some.other.setting", "some.other.value"),
        jobContext.getInputSettings("source-users"));

    Assert.assertEquals(
        ImmutableMap.of("my.output.setting", "my.output.value",
            "some.other.setting", "some.other.value"),
        jobContext.getOutputSettings("target-users"));
  }

  @Test
  public void testHadoopSettings() {

    AppContext context = context ("hadoop.mapreduce.map.memory.mb", "2048",
        "kite.job." + JOB_NAME + ".hadoop.mapreduce.reduce.memory.mb", "4096");

    Configuration conf = jobContext(context).getHadoopConf();

    Assert.assertEquals("2048", conf.get("mapreduce.map.memory.mb"));
    Assert.assertEquals("4096", conf.get("mapreduce.reduce.memory.mb"));
  }

  @Test
  public void testStreamSettings() {


    AppContext context = context();

    StreamDescription descrip = new StreamDescription.Builder()
        .jobName(JOB_NAME)
        .jobClass(MockStreamingJob.class)
        .withStream("mock_input", ImmutableMap.of("input_stream.setting", "input_stream.value",
            "other.stream.setting", "other.stream.value"))
        .withStream("mock_output", ImmutableMap.of("output_stream.setting", "output_stream.value"))
        .build();

    Job job = new MockStreamingJob();

    JobContext jobContext = new JobContext(descrip,
        Collections.<String,String>emptyMap(),
        context.getHadoopConf());

    Assert.assertEquals(
        ImmutableMap.of("input_stream.setting", "input_stream.value",
            "other.stream.setting", "other.stream.value"),
        jobContext.getInputSettings("mock_input"));

    Assert.assertEquals(
        ImmutableMap.of("output_stream.setting", "output_stream.value"),
        jobContext.getOutputSettings("mock_output"));


    context = context("kite.job." + JOB_NAME + ".input.mock_input.input_stream.setting", "overridden_value");

    // test overridden settings
    jobContext = new JobContext(descrip,
        context.getSettings(),
        context.getHadoopConf());

    Assert.assertEquals(
        ImmutableMap.of("input_stream.setting", "overridden_value",
            "other.stream.setting", "other.stream.value"),
        jobContext.getInputSettings("mock_input"));

    Assert.assertEquals(
        ImmutableMap.of("output_stream.setting", "output_stream.value"),
        jobContext.getOutputSettings("mock_output"));
  }
}
