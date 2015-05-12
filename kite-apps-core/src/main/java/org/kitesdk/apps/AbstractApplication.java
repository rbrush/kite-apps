package org.kitesdk.apps;

import com.google.common.collect.Lists;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.Datasets;

import java.util.Collections;
import java.util.List;

/**
 * Base class to simplify common application patterns.
 */
public abstract class AbstractApplication implements Application {

  private final List<Schedule> schedules = Lists.newArrayList();

  public void createDataset(String uri, DatasetDescriptor descriptor) {}

  protected void schedule(Schedule schedule) {

    schedules.add(schedule);
  };

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

  public List<Schedule> getSchedules() {

    return Collections.unmodifiableList(schedules);
  }
}
