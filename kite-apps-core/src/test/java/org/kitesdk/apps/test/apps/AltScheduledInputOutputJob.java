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
package org.kitesdk.apps.test.apps;

import com.google.common.io.Closeables;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;

/**
 * Alternate scheduled job for testing.
 */
public class AltScheduledInputOutputJob extends AbstractSchedulableJob {

  public void run(@DataIn(name="source_users") View<GenericRecord> input,
                  @DataOut(name="target_users") View<GenericRecord> output) {

    DatasetReader<GenericRecord> reader = input.newReader();
    DatasetWriter<GenericRecord> writer = output.newWriter();

    try {
      while (reader.hasNext()) {

        writer.write(reader.next());
      }
    } finally {

      Closeables.closeQuietly(reader);
      Closeables.closeQuietly(writer);
    }
  }
}
