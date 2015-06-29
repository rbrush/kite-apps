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
package org.kitesdk.apps.spark;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.spark.apps.SimpleSparkApp;
import org.kitesdk.apps.test.TestScheduler;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;
import org.kitesdk.apps.spark.spi.DefaultSparkContext;

import java.net.URI;
import java.util.List;

/**
 * Simple test of a Spark-based application.
 */
public class SimpleSparkAppTest extends MiniDFSTest {

  @Before
  public void setDefaultConfig() {
    DefaultConfiguration.set(getConfiguration());

    SparkConf conf = new SparkConf()
        .setMaster("local[3]")
        .setAppName("spark-test");

    DefaultSparkContext.setContext(new JavaSparkContext(conf));
  }

  @After
  public void stopContext() {

    DefaultSparkContext.getContext().stop();
    DefaultSparkContext.setContext(null);
  }

  @Test
  public void testSimpleMap() {

    TestScheduler scheduler = TestScheduler.load(SimpleSparkApp.class, getConfiguration());

    Instant nominalTime = new DateTime(2015, 5, 15, 12, 0, 0, 0, DateTimeZone.UTC).toInstant();

    Dataset<GenericData.Record> in = Datasets.load(SimpleSparkApp.INPUT_DATASET,
        GenericData.Record.class);

    Dataset<GenericData.Record> out = Datasets.load(SimpleSparkApp.OUTPUT_DATASET,
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

}
