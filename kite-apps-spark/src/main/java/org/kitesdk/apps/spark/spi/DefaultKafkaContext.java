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
package org.kitesdk.apps.spark.spi;

/**
 * Default context for Kafka consuming applications.
 */
public class DefaultKafkaContext {

  private static volatile String zkConnectionString;

  private static volatile String kafkaBrokerList;

  public static String getZookeeperConnectionString() {
    return zkConnectionString;
  }

  public static void setZookeeperConnectionString(String zkConnectionString) {
    DefaultKafkaContext.zkConnectionString = zkConnectionString;
  }

  public static String getKafkaBrokerList() {
    return kafkaBrokerList;
  }

  public static void setKafkaBrokerList(String kafkaBrokerList){

    DefaultKafkaContext.kafkaBrokerList = kafkaBrokerList;
  }
}
