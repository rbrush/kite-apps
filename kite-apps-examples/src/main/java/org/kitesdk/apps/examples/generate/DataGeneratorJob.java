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
package org.kitesdk.apps.examples.generate;


import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.DataOut;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Signalable;

/**
 * Job that simply writes test data on a pre-defined schedule.
 */
public class DataGeneratorJob extends AbstractSchedulableJob {

  @Override
  public String getName() {
    return "example-data-generator";
  }

  public void run(@DataOut(name="example_events", type=ExampleEvent.class) Signalable<ExampleEvent> view) {

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
