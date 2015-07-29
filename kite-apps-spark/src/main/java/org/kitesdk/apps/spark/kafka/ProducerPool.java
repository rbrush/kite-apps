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
package org.kitesdk.apps.spark.kafka;

import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.List;
import java.util.Properties;

/**
 * A pool of reusable Kafka producers to be used in a job.
 */
class ProducerPool {

  private final ProducerConfig config;

  private final List<Producer> producers = Lists.newArrayList();

  /**
   * Gets a producer pool that uses the given Kafka properties.
   */
  public static ProducerPool get(Properties props) {

    // TODO: reuse producer pools with matching properties?
    return new ProducerPool(new ProducerConfig(props));
  }

  public ProducerPool(ProducerConfig config) {
    this.config = config;
  }

  public synchronized Producer get() {

    if (producers.size() > 0)
      return producers.remove(0);

    Producer producer = new Producer(config);

    return producer;
  }

  public synchronized void release(Producer producer) {
    producers.add(producer);
  }
}
