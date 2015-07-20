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
package org.kitesdk.apps;

import com.google.common.collect.Lists;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.Datasets;

import java.util.Collections;
import java.util.List;

/**
 * Base class to simplify common application patterns. See {@link Application}
 * for details on use.
 */
public abstract class AbstractApplication implements Application {

  private final List<Schedule> schedules = Lists.newArrayList();

  private final List<StreamDescription> descriptions = Lists.newArrayList();

  protected void schedule(Schedule schedule) {

    schedules.add(schedule);
  };

  protected void stream(StreamDescription description) {

    descriptions.add(description);
  }

  /**
   * Ensures the given dataset exists, creating it if it doesn't
   * and updating the schema if necessary.
   */
  protected void dataset(String uri, DatasetDescriptor descriptor) {

    try {

      Datasets.create(uri, descriptor);
    } catch (DatasetExistsException e) {

      Dataset existingDataset = Datasets.load(uri);

      DatasetDescriptor updated;

      // The given discriptor might not have a location,
      // so use the current one.
      if (descriptor.getLocation() == null) {
        updated = new DatasetDescriptor.Builder(descriptor)
            .location(existingDataset.getDescriptor().getLocation())
            .build();
      } else {

        updated = descriptor;
      }

      Datasets.update(uri, updated);
    }
  }

  @Override
  public List<Schedule> getSchedules() {

    return Collections.unmodifiableList(schedules);
  }

  public List<StreamDescription> getStreamDescriptions() {

    return Collections.unmodifiableList(descriptions);
  }
}
