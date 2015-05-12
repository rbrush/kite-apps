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

    Dataset<ExampleEvent> eventDataset = Datasets.load(DataGeneratorApp.EVENT_DS_URI, ExampleEvent.class);

    Dataset<ExampleEvent> oddUserDataset = Datasets.load(TriggeredApp.ODD_USER_DS_URI, ExampleEvent.class);

    DateTime firstNominalTime = new DateTime(2015, 5, 7, 12, 0, 0);

    // Run the generator job at each minute.
    for (int i = 0; i < 2; ++i) {

      Instant nominalTime = firstNominalTime.plusMinutes(i).toInstant();
      DateTime dateTime = nominalTime.toDateTime(DateTimeZone.UTC);

      generatorRunner.runScheduledJobs(nominalTime,
          "example.events", eventDataset);

      View<ExampleEvent> output =  oddUserDataset.with("year", dateTime.getYear())
          .with("month", dateTime.getMonthOfYear())
          .with("day", dateTime.getDayOfMonth())
          .with("hour", dateTime.getHourOfDay())
          .with("minute", dateTime.getMinuteOfHour());

      // TODO: a testing facility should create the dataset based on the nominal
      // time and URI pattern, but we wire things ourselves for now.
      triggeredRunner.runScheduledJobs(nominalTime,
          "example.events", eventDataset,
          "odd.users", output);


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
