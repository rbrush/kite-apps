package org.kitesdk.apps.examples.generate;


import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Signalable;

/**
 * Job that simply writes test data on a pre-defined schedule.
 */
public class DataGeneratorJob extends AbstractSchedulableJob {

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
