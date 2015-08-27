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
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.scheduled.AbstractSchedulableJob;
import org.kitesdk.apps.test.KeyValues;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;

/**
 * Test job that simply writes configuration for validation purposes.
 */
public class WriteConfigOutputJob extends AbstractSchedulableJob {

  public void run(@DataOut(name="kv-output", type= KeyValues.class) View<KeyValues> output) {


    DatasetWriter<KeyValues> writer = output.newWriter();

    try {

      JobContext context = getJobContext();

      KeyValues kv = KeyValues.newBuilder()
          .setJobsettings(context.getSettings())
          .setOutputsettings(context.getOutputSettings("kv-output"))
          .build();

      writer.write(kv);

    } finally {


      Closeables.closeQuietly(writer);
    }
  }
}
