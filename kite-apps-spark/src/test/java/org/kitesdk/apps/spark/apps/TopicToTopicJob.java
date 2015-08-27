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

import org.apache.spark.streaming.api.java.JavaDStream;
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.apps.spark.AbstractStreamingSparkJob;
import org.kitesdk.apps.spark.kafka.KafkaOutput;
import org.kitesdk.data.event.SmallEvent;

/**
 * Test job that simply moves data from one Kafka topic to another.
 */
public class TopicToTopicJob extends AbstractStreamingSparkJob {

  public void run(@DataIn(name = "event_stream", type = SmallEvent.class)
                  JavaDStream<SmallEvent> stream,
                  @DataOut(name = "processed_stream", type = SmallEvent.class)
                  KafkaOutput<SmallEvent> output) {

    output.write(stream);
  }
}
