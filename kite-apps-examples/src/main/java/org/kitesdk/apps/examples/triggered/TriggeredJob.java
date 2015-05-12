package org.kitesdk.apps.examples.triggered;

import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.kitesdk.apps.crunch.AbstractCrunchJob;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;

/**
 * Simple job that uses a Crunch pipeline to filter out a set of users.
 */
public class TriggeredJob extends AbstractCrunchJob {

  public static class KeepOddUsers extends FilterFn<ExampleEvent> {

    @Override
    public boolean accept(ExampleEvent event) {
      // Keep only odd users.
      return event.getUserId() % 2 == 1;
    }
  }

  public void run(@DataIn(name="example.events", type=ExampleEvent.class) View<ExampleEvent> input,
                  @DataOut(name="odd.users", type=ExampleEvent.class) View<ExampleEvent> output) {

    Pipeline pipeline = getPipeline();

    PCollection<ExampleEvent> events = pipeline.read(CrunchDatasets.asSource(input));

    PCollection<ExampleEvent> oddUsers = events.filter(new KeepOddUsers());

    pipeline.write(oddUsers, CrunchDatasets.asTarget(output));

    pipeline.run();
  }
}
