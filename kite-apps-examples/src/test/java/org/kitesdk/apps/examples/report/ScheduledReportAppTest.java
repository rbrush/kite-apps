package org.kitesdk.apps.examples.report;

import org.apache.avro.generic.GenericData;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.apps.MiniAppTest;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.examples.generate.DataGeneratorApp;
import org.kitesdk.apps.test.TestScheduler;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Datasets;

import org.kitesdk.data.View;

import java.util.Collections;


public class ScheduledReportAppTest extends MiniAppTest {

  @Test
  public void testGenerateAndRunReport() {

    TestScheduler generatorRunner = TestScheduler.load(DataGeneratorApp.class, getConfiguration());

    TestScheduler reportRunner = TestScheduler.load(ScheduledReportApp.class, getConfiguration());

    Dataset<ExampleEvent> eventDataset = Datasets.load(DataGeneratorApp.EVENT_DS_URI, ExampleEvent.class);

    DateTime firstNominalTime = new DateTime(2015, 5, 7, 12, 0, 0);

    // Run the generator job at each minute.
    for (int i = 0; i < 5; ++i) {
      generatorRunner.runScheduledJobs(firstNominalTime.plusMinutes(i).toInstant(),
          Collections.<String,View>singletonMap("example.events", eventDataset));
    }

    // Now run the report job to aggregate over the schedule.
    reportRunner.runScheduledJobs(firstNominalTime.plusMinutes(5).toInstant());


    // Verify the expected data was written.
    Dataset<GenericData.Record> ds = Datasets.load(ScheduledReportApp.REPORT_DS_URI, GenericData.Record.class);

    DatasetReader<GenericData.Record> reader = ds.newReader();

    try {

      int count = 0;

      for (GenericData.Record event: reader) {

        System.out.println(event.get("user_id"));

        // Each had an event created in each generated data run,
        // totalling 5
        Assert.assertEquals(5L, event.get("event_count"));

        ++count;
      }

      // We should see ten distinct IDs.
      Assert.assertEquals(10, count);

    } finally {
      reader.close();
    }
  }
}
