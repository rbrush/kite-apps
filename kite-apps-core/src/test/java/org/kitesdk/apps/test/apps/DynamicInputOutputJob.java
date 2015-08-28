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
import org.joda.time.Instant;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.JobParameters;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.View;

import java.util.Map;

public class DynamicInputOutputJob implements SchedulableJob {

  @Override
  public JobParameters getParameters() {
    // Dynamic job specifies no parameters
    return null;
  }

  @Override
  public void runJob(Map params, JobContext jobContext, Instant nominalTime) {

    View output = (View) params.get("output");

    DatasetWriter<GenericRecord> writer = output.newWriter();

    // Simply write all of the input datasets to the output.
    for (Map.Entry<String,View> param: ((Map<String,View>)  params).entrySet()) {

      if (!param.getKey().equals("output")) {
        DatasetReader<GenericRecord> reader = param.getValue().newReader();

        try {
          while (reader.hasNext()) {

            writer.write(reader.next());
          }
        } finally {

          Closeables.closeQuietly(reader);

        }
      }
    }

    Closeables.closeQuietly(writer);
  }
}
