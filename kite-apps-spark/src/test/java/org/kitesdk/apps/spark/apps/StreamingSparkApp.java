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
package org.kitesdk.apps.spark.apps;

import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.spark.kafka.Topics;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.event.SmallEvent;

public class StreamingSparkApp extends AbstractApplication {

  /**
   * Name of the input topic.
   */
  public static final String TOPIC_NAME = "example_events";

  /**
   * URI of the dataset created by this application.
   */
  public static final String EVENTS_DS_URI = "dataset:hdfs:///tmp/sparkstreamtest/sparkevents";

  @Override
  public void setup(AppContext context) {

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(SmallEvent.class)
        .build();

    dataset(EVENTS_DS_URI, descriptor);

    Topics.createTopic(context, TOPIC_NAME, 1, 1, SmallEvent.getClassSchema());

    StreamDescription streamDescription = new StreamDescription.Builder()
        .jobName("simple-spark-streaming")
        .jobClass(StreamingSparkJob.class)
        .withStream("event_stream", Topics.topic(TOPIC_NAME))
        .withView("event_output", EVENTS_DS_URI)
        .build();

    stream(streamDescription);
  }
}
