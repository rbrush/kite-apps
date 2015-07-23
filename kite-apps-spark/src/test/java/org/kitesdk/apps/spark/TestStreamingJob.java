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

import com.clearspring.analytics.util.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.spark.apps.StreamingSparkApp;
import org.kitesdk.apps.spark.apps.StreamingSparkJob;
import org.kitesdk.apps.spark.spi.SparkContextFactory;
import org.kitesdk.apps.spark.spi.streaming.SparkStreamingJobManager;
import org.kitesdk.apps.spark.test.SparkKafkaTestHarness;
import org.kitesdk.apps.spi.AppDeployer;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.View;
import org.kitesdk.data.event.SmallEvent;
import org.kitesdk.data.spi.DefaultConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 *
 */
public class TestStreamingJob extends MiniDFSTest {


  private SparkKafkaTestHarness harness;

  @Before
  public void setupRunner() throws Exception {

    DefaultConfiguration.set(getConfiguration());

    harness = new SparkKafkaTestHarness(StreamingSparkApp.class, getConfiguration());
  }

  @After
  public void tearDown() throws IOException {
    harness.tearDown();
    SparkContextFactory.shutdown();
  }

  /**
   * Loads the expected number of items when they are available in the dataset.
   * @param expected
   * @return
   */
  private <T> List<T> loadWhenAvailable(View<T> view, int expected) {

    for (int attempt = 0; attempt < 20; ++attempt) {

      List<T> items = Lists.newArrayList();

      DatasetReader<T> reader = view.newReader();
      int count = 0;

      try {

        for (; count < expected; ++count) {

          if (!reader.hasNext())
            continue;


          items.add(reader.next());
        }

      } finally {
        reader.close();
      }

      if (count == expected) {
        return items;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    Assert.fail("Unable to load the expected items.");
    return null;
  }

  @Test
  public void testJob() throws IOException {

    List<SmallEvent> events = Lists.newArrayList();

    for (int i = 0; i < 10; ++i) {
      SmallEvent event = SmallEvent.newBuilder()
          .setSessionId("1234")
          .setUserId(i)
          .build();

      events.add(event);
    }

    harness.writeMessages(StreamingSparkApp.TOPIC_NAME, events);

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    View<SmallEvent> view = Datasets.load(StreamingSparkApp.EVENTS_DS_URI, SmallEvent.class);

    List<SmallEvent> results = loadWhenAvailable(view, 10);

    Assert.assertEquals(events, results);
  }

  private Path createTestDirectory() {
    String tempBase = getConfiguration().get("hadoop.tmp.dir", "/tmp");

    String tempName = "testapp-" + (new Random().nextInt() & Integer.MAX_VALUE);
    return getFS().makeQualified(new Path(tempBase, tempName));
  }

  private static File createTestProperties() throws IOException {

    File propFile = File.createTempFile("test", "properties");

    Properties props = new Properties();

    props.put("key1", "val1");
    props.put("key2", "val2");

    FileOutputStream output = new FileOutputStream(propFile);

    try {

      props.store(output, "");

    } finally {
      output.close();
    }

    return propFile;
  }

  @Test
  public void testDeploySimpleApp() throws Exception {

    FileSystem fs = getFS();

    Path testDirectory = createTestDirectory();

    AppDeployer deployer = new AppDeployer(fs, harness.getAppContext());

    StreamingSparkApp app = new StreamingSparkApp();

    app.setup(harness.getAppContext());

    File propFile = createTestProperties();

    deployer.install(app, testDirectory, propFile, Collections.<File>emptyList());

    Path confFile = new Path(testDirectory, "conf/app.properties");
    junit.framework.Assert.assertTrue(fs.exists(confFile));

    String jobName = new StreamingSparkJob().getName();

    Path streamDescription = SparkStreamingJobManager.jobDescriptionFile(testDirectory,
        jobName);

    Assert.assertTrue(fs.exists(streamDescription));

    InputStream input = fs.open(streamDescription);

    try {

      InputStreamReader streamReader = new InputStreamReader(input);
      BufferedReader reader = new BufferedReader(streamReader);

      StringBuilder builder = new StringBuilder();

      String line = null;

      while ((line = reader.readLine()) != null) {
        builder.append(line);
      }

      StreamDescription description = StreamDescription.parseJson(builder.toString());

      Assert.assertEquals(jobName, description.getJobName());
      Assert.assertEquals(StreamingSparkJob.class, description.getJobClass());

    } finally {
      Closeables.closeQuietly(input);
    }
  }
}
