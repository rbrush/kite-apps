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
package org.kitesdk.apps.examples.streaming.totopic;

import org.kitesdk.apps.AbstractApplication;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.apps.spark.kafka.Topics;
import org.kitesdk.apps.streaming.StreamDescription;

public class TopicToTopicApp extends AbstractApplication {

  /**
   * Name of the input topic.
   */
  public static final String INPUT_TOPIC = "example_input_events";

  /**
   * Name of the output topic.
   */
  public static final String OUTPUT_TOPIC = "example_output_events";

  @Override
  public void setup(AppContext context) {

    String environment = context.getSettings().get("environment");

    if (environment == null) {
      throw new IllegalArgumentException("An environment setting must be provided for this application.");
    }

    String inputTopic = environment + "." + INPUT_TOPIC;
    String outputTopic = environment + "." + OUTPUT_TOPIC;

    Topics.createTopic(context, inputTopic, 1, 1, ExampleEvent.getClassSchema());
    Topics.createTopic(context, outputTopic, 1, 1, ExampleEvent.getClassSchema());

    StreamDescription processJob = new StreamDescription.Builder()
        .jobClass(TopicToTopicJob.class)
        .withStream("example_input_stream", Topics.topic(inputTopic))
        .withStream("example_output_stream", Topics.topic(outputTopic))
        .build();

    stream(processJob);
  }
}
