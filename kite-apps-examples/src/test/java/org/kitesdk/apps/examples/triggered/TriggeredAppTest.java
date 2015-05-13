package org.kitesdk.apps.examples.triggered;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
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


public class TriggeredAppTest extends MiniAppTest {

  @Test
  public void testTriggeredApp() {

    TestScheduler generatorRunner = TestScheduler.load(DataGeneratorApp.class, getConfiguration());

    TestScheduler triggeredRunner = TestScheduler.load(TriggeredApp.class, getConfiguration());

    DateTime firstNominalTime = new DateTime(2015, 5, 7, 12, 0, 0);

    // Run the generator job at each minute.
    for (int i = 0; i < 2; ++i) {

      Instant nominalTime = firstNominalTime.plusMinutes(i).toInstant();
      DateTime dateTime = nominalTime.toDateTime(DateTimeZone.UTC);

      // Generate the data and then run the triggered job, which should read it.
      generatorRunner.runScheduledJobs(nominalTime);
      triggeredRunner.runScheduledJobs(nominalTime);

      // Get the output at the expected time and read its contents.
      Dataset<ExampleEvent> oddUserDataset = Datasets.load(TriggeredApp.ODD_USER_DS_URI, ExampleEvent.class);

      View<ExampleEvent> output =  oddUserDataset.with("year", dateTime.getYear())
          .with("month", dateTime.getMonthOfYear())
          .with("day", dateTime.getDayOfMonth())
          .with("hour", dateTime.getHourOfDay())
          .with("minute", dateTime.getMinuteOfHour());

      // Verify the output contains only the expected user IDs.
      DatasetReader<ExampleEvent> reader = output.newReader();

      try {

        int count = 0;

        for (ExampleEvent event: reader) {

          System.out.println(event.getUserId());

          Assert.assertTrue(event.getUserId() % 2 == 1);

          ++count;
        }

      } finally {
        reader.close();
      }
    }
  }
}
