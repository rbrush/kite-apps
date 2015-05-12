package org.kitesdk.apps.examples.generate;


import org.joda.time.DateTimeZone;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.scheduled.AbstractScheduledJob;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.View;

/**
 * Job that simply writes test data on a pre-defined schedule.
 */
public class DataGeneratorJob extends AbstractScheduledJob {

  public void run(@DataOut(name="example.events", type=ExampleEvent.class) Signalable<ExampleEvent> view) {

    // Write some test data to the view.
    DatasetWriter<ExampleEvent> writer = view.newWriter();

    try {

      for (int i = 0; i < 10; ++i) {

        ExampleEvent event = ExampleEvent.newBuilder()
            .setUserId(i)
            .setSessionId(Integer.toString(i))
            .setTimestamp(getNominalTime().getMillis())
            .build();

        writer.write(event);
      }
    }
    finally {
      writer.close();
    }

    // Signal that our view is ready.
    view.signalReady();
  }
}
