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
package org.kitesdk.apps.scheduled;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.avro.generic.GenericData;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.test.KeyValues;
import org.kitesdk.apps.test.TestScheduler;
import org.kitesdk.apps.test.apps.ScheduledInputOutputApp;
import org.kitesdk.apps.test.apps.WriteConfigOutputApp;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class SchedulableJobTest extends MiniDFSTest {

  @Before
  public void setDefaultConfig() {
    DefaultConfiguration.set(getConfiguration());
  }

  @Test
  public void testSimpleMap() {

    AppContext context = new AppContext(getConfiguration());

    TestScheduler scheduler = TestScheduler.load(ScheduledInputOutputApp.class, context);

    Instant nominalTime = new DateTime(2015, 5, 15, 12, 0, 0, 0, DateTimeZone.UTC).toInstant();

    Dataset<GenericData.Record> in = Datasets.load(ScheduledInputOutputApp.INPUT_DATASET,
        GenericData.Record.class);

    Dataset<GenericData.Record> out = Datasets.load(ScheduledInputOutputApp.OUTPUT_DATASET,
        GenericData.Record.class);

    View<GenericData.Record> input = in.with("year", 2015)
        .with("month", 5)
        .with("day", 15)
        .with("hour", 12);

    DatasetTestUtilities.writeTestUsers(input, 10);

    scheduler.runScheduledJobs(nominalTime);

    DatasetTestUtilities.checkTestUsers(out, 10);

    // Make sure only the expected partition is present.
    List<PartitionView> partitions = Lists.newArrayList();

    for (PartitionView partition:  out.getCoveringPartitions()) {
      partitions.add(partition);
    }

    Assert.assertEquals(1, partitions.size());

    URI uri =  partitions.get(0).getUri();

    Assert.assertTrue(uri.toString().endsWith("year=2015&month=5&day=15&hour=12"));
  }

  @Test
  public void testJobConfiguration() {

    Map<String,String> settings = ImmutableMap.of("test.app.level.setting", "appvalue",
        "kite.job.write-config-job.test.job.level.setting", "jobvalue",
        "kite.job.write-config-job.output.kv-output.test.output.level.setting", "outputvalue");

    AppContext context = new AppContext(settings, getConfiguration());

    TestScheduler scheduler = TestScheduler.load(WriteConfigOutputApp.class, context);

    Instant nominalTime = new DateTime(2015, 5, 15, 12, 0, 0, 0, DateTimeZone.UTC).toInstant();

    scheduler.runScheduledJobs(nominalTime);

    Dataset<KeyValues> ds = Datasets.load(WriteConfigOutputApp.OUTPUT_DATASET, KeyValues.class);

    DatasetReader<KeyValues> reader = ds.newReader();

    try {

      KeyValues kv = reader.next();

      Assert.assertEquals(ImmutableMap.of(
              "test.app.level.setting", "appvalue",
              "test.job.level.setting", "jobvalue",
              "output.kv-output.test.output.level.setting", "outputvalue"),
          kv.getJobsettings());

      Assert.assertEquals(ImmutableMap.of(
              "test.app.level.setting", "appvalue",
              "test.job.level.setting", "jobvalue",
              "test.output.level.setting", "outputvalue"),
          kv.getOutputsettings());

    } finally {
      Closeables.closeQuietly(reader);
    }
  }

}
