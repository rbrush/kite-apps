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
package org.kitesdk.apps.spark;

import com.google.common.collect.Maps;
import kafka.admin.AdminUtils;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.kitesdk.apps.spark.spi.DefaultKafkaContext;
import org.kitesdk.apps.spark.spi.DefaultSparkContext;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaUtils {

  public static final String TOPIC_NAME = "topic";


  public static Map<String,String> kafkaProps(String topic) {

    Map<String,String> props = Maps.newHashMap();

    props.put(TOPIC_NAME, topic);

    return props;
  }

  public static void createTopic(String topicName) {

    ZkClient client = new ZkClient(DefaultKafkaContext.getZookeeperConnectionString(), 1000, 1000, ZKStringSerializer$.MODULE$);

    if (!AdminUtils.topicExists(client, topicName)) {
      AdminUtils.createTopic(client, topicName, 1, 1, new Properties());
    }

    // Hack to poll for availability of the topic's metadata, since some users
    // may immediately use the topic after creating it.
    String host = DefaultKafkaContext.getKafkaBrokerList().split(":")[0];
    int port = Integer.parseInt(DefaultKafkaContext.getKafkaBrokerList().split(":")[1]);

    SimpleConsumer consumer = new SimpleConsumer(host, port, 1000, 10000, "test");

    for (int i = 0; i < 10; ++i) {
      TopicMetadataResponse response = consumer.send(new TopicMetadataRequest(Collections.singletonList(topicName)));

      if (response.topicsMetadata().size() > 0)
        break;

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
