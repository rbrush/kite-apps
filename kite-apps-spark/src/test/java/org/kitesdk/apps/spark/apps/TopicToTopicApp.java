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
import org.kitesdk.data.event.SmallEvent;

public class TopicToTopicApp extends AbstractApplication {

  /**
   * Name of the input topic.
   */
  public static final String EVENT_TOPIC_NAME = "example_events";

  /**
   * Name of the input topic.
   */
  public static final String PROCESSED_TOPIC_NAME = "processed_events";


  @Override
  public void setup(AppContext context) {

    Topics.createTopic(context, EVENT_TOPIC_NAME, 1, 1, SmallEvent.getClassSchema());
    Topics.createTopic(context, PROCESSED_TOPIC_NAME, 1, 1, SmallEvent.getClassSchema());

    StreamDescription processJob = new StreamDescription.Builder()
        .jobName("topic-to-topic")
        .jobClass(TopicToTopicJob.class)
        .withStream("event_stream", Topics.topic(EVENT_TOPIC_NAME))
        .withStream("processed_stream", Topics.topic(PROCESSED_TOPIC_NAME))
        .build();

    stream(processJob);
  }
}
