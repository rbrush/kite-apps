package org.kitesdk.apps.examples.generate;

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.apps.MiniAppTest;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.test.TestScheduler;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;

import java.util.Collections;

/**
 * Test the data generator app.
 */
public class DataGeneratorAppTest extends MiniAppTest {

  @Test
  public void testGenerateData() {

    TestScheduler container = TestScheduler.load(DataGeneratorApp.class, getConfiguration());

    container.runScheduledJobs(new Instant());

    // Verify the expected data was written.
    Dataset<ExampleEvent> ds = Datasets.load(DataGeneratorApp.EVENT_DS_URI, ExampleEvent.class);

    DatasetReader<ExampleEvent> reader = ds.newReader();

    try {

      int count = 0;

      for (ExampleEvent event: reader) {

        ++count;
      }

      Assert.assertEquals(10, count);

    } finally {
      reader.close();
    }
  }
}
