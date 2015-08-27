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
package org.kitesdk.apps.examples.triggered;

import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.kitesdk.apps.crunch.AbstractCrunchJob;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;

/**
 * Simple job that uses a Crunch pipeline to filter out a set of users.
 */
public class TriggeredJob extends AbstractCrunchJob {

  public static class KeepOddUsers extends FilterFn<ExampleEvent> {

    private static final long serialVersionUID = 1;

    @Override
    public boolean accept(ExampleEvent event) {
      // Keep only odd users.
      return event.getUserId() % 2 == 1;
    }
  }

  public void run(@DataIn(name="example_events", type=ExampleEvent.class) View<ExampleEvent> input,
                  @DataOut(name="odd_users", type=ExampleEvent.class) View<ExampleEvent> output) {

    Pipeline pipeline = getPipeline();

    PCollection<ExampleEvent> events = pipeline.read(CrunchDatasets.asSource(input));

    PCollection<ExampleEvent> oddUsers = events.filter(new KeepOddUsers());

    pipeline.write(oddUsers, CrunchDatasets.asTarget(output));

    pipeline.run();
  }
}
