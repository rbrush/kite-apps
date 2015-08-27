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
package org.kitesdk.apps.streaming;

import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

public class StreamDescriptionTest {

  @Test
  public void testCreateDescription() {

    Map<String,String> props = Maps.newHashMap();
    props.put("setting1", "value1");
    props.put("setting2", "value2");

    StreamDescription descrip = new StreamDescription.Builder()
        .jobName("mock-job")
        .jobClass(MockStreamingJob.class)
        .withStream("example.stream", props)
        .withView("example.view", "dataset:hive:example/view").build();

    Assert.assertEquals(descrip.getJobName(), descrip.getJobName());

    Assert.assertEquals(MockStreamingJob.class, descrip.getJobClass());

    Map<String, StreamDescription.Stream> streams = descrip.getStreams();

    Assert.assertEquals(1, streams.size());

    StreamDescription.Stream stream = streams.get("example.stream");

    Assert.assertEquals(props, stream.getProperties());

    Map<String,URI> viewUris = descrip.getViewUris();

    Assert.assertEquals(URI.create("dataset:hive:example/view"),
        viewUris.get("example.view"));
  }

  @Test
  public void testSerialization() {

    Map<String,String> props = Maps.newHashMap();
    props.put("setting1", "value1");
    props.put("setting2", "value2");

    StreamDescription descrip = new StreamDescription.Builder()
        .jobName("mock-job")
        .jobClass(MockStreamingJob.class)
        .withStream("example.stream", props)
        .withView("example.view", "dataset:hive:example/view").build();

    String json = descrip.toString();

    StreamDescription readDescrip = StreamDescription.parseJson(json);

    Assert.assertEquals(descrip, readDescrip);
  }
}
