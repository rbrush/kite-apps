package org.kitesdk.apps.spi.oozie;

import org.apache.avro.generic.GenericData;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.data.View;

public class TestJob extends AbstractSchedulableJob {

  public void run(@DataIn(name="input.data", type=GenericData.Record.class) View<GenericData.Record> input,
                  @DataOut(name="output.data", type=GenericData.Record.class) View<GenericData.Record> output) {

    // Job logic goes here...

  }

}
