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

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Schema;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobContext;

import java.util.Map;
import java.util.Properties;

public class KafkaUtils {

  public static final String TOPIC_NAME = "topic.name";

  public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";

  public static final String BROKER_LIST = "metadata.broker.list";

  private static final String PREFIX = "kafka.";

  private static String getKafkaProp(AppContext context, String property) {

    String kafkaPropName = PREFIX + property;

    String value = context.getSettings().get(kafkaPropName);

    if (value == null)
      throw new AppException("Required property " + kafkaPropName + " does not exist.");

    return value;
  }

  private static String getKafkaProp(JobContext context, String property) {

    String kafkaPropName = PREFIX + property;

    String value = context.getSettings().get(kafkaPropName);

    if (value == null)
      throw new AppException("Required property " + kafkaPropName + " does not exist.");

    return value;
  }

  /**
   * Helper function to create settings that can be used when defining
   * a job input or output.
   */
  public static Map<String,String> kafkaProps(String topic) {

    Map<String,String> props = Maps.newHashMap();

    props.put(TOPIC_NAME, topic);

    return props;
  }

  public static Map<String,String> getDirectStreamParams(SparkJobContext context) {

    Map<String,String> params = Maps.newHashMap();

    params.put("metadata.broker.list", getKafkaProp(context, BROKER_LIST));
    params.put("zookeeper.connect", getKafkaProp(context, ZOOKEEPER_CONNECT));
    params.put("group.id", "test_group");
    params.put("client.id", "test_client");
    params.put("socket.timeout.ms", "500");
    params.put("consumer.id", "test");
    params.put("auto.offset.reset", "smallest");
    params.put("retry.backoff.ms", Integer.toString(500));
    params.put("message.send.max.retries", Integer.toString(20));

    return params;
  }

  public static void createTopic(AppContext context,
                                 String topicName,
                                 int partitions,
                                 int replicationFactor,
                                 Schema schema) {

    // Currently the schema parameter is unused, but we have it here so it can
    // be applied to a schema registry for the topic when that is available.

    ZkClient client = new ZkClient(getKafkaProp(context, ZOOKEEPER_CONNECT), 1000, 1000, ZKStringSerializer$.MODULE$);

    try {
      if (!AdminUtils.topicExists(client, topicName)) {
        AdminUtils.createTopic(client, topicName, partitions, replicationFactor, new Properties());
      }
    } finally {
      client.close();
    }
  }
}
