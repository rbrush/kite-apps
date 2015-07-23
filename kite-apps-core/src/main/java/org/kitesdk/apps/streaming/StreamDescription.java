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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import org.kitesdk.apps.AppException;
import org.kitesdk.data.ValidationException;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * Description for a streaming job.
 */
public class StreamDescription {

  private final Class<? extends StreamingJob> jobClass;

  private final String jobName;

  private final Map<String,Stream> streams;

  private final Map<String,URI> viewUris;

  public  Class<? extends StreamingJob> getJobClass() {
    return jobClass;
  }

  public String getJobName() {
    return jobName;
  }

  public Map<String,Stream> getStreams() {
    return streams;
  }

  public Map<String,URI> getViewUris() {return viewUris;}

  private StreamDescription(Class<? extends StreamingJob> jobClass,
                            String jobName,
                            Map<String,Stream> streams,
                            Map<String,URI> viewUris) {
    this.jobClass = jobClass;
    this.jobName = jobName;
    this.streams = streams;
    this.viewUris = viewUris;
  }

  public static class Stream {

    private final String name;

    private final Map<String,String> properties;

    Stream(String name, Map<String,String> properties) {
      this.name = name;
      this.properties = properties;
    }

    public String getName() {return name;}

    public Map<String,String> getProperties() {
      return properties;
    }

    public boolean equals(Object that) {

      if (this == that) {
        return true;
      }

      if (that == null || !(that instanceof Stream)) {
        return false;
      }

      Stream _that = (Stream) that;

      return name.equals(_that.name) &&
          properties.equals(_that.properties);
    }

    public int hashCode() {

      return 37 * name.hashCode() * properties.hashCode();
    }
  }

  /**
   * Builder class for stream descriptions.
   */
  public static class Builder {

    private Class<? extends StreamingJob> jobClass;

    private String jobName;

    private final Map<String,Stream> streams = Maps.newHashMap();

    private final Map<String,URI> viewUris = Maps.newHashMap();

    /**
     * Sets the class of the {@link org.kitesdk.apps.streaming.StreamingJob}
     * being configured.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder jobClass(Class<? extends StreamingJob> jobClass) {

      this.jobClass = jobClass;

      try {
        StreamingJob job = jobClass.newInstance();
        jobName = job.getName();

      } catch (InstantiationException e) {
        throw new AppException(e);
      } catch (IllegalAccessException e) {
        throw new AppException(e);
      }

      return this;
    }

    /**
     * Defines a stream for use in the job.
     *
     * @param name
     * @param properties
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder withStream(String name, Map<String,String> properties) {

      Stream stream = new Stream(name, properties);

      streams.put(name, stream);

      return this;

    }

    /**
     * Defines a view for use in the job.
     *
     * @param name
     * @param uri
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder withView(String name, String uri) {

      viewUris.put(name,URI.create(uri));

      return this;
    }

    /**
     * Builds the description, returning an immutable
     * {@link org.kitesdk.apps.streaming.StreamDescription} instance.
     *
     * @return a StreamDescription.
     */
    public StreamDescription build() {
      return new StreamDescription(jobClass, jobName, streams, viewUris);
    }
  }

  private static String STREAMS = "streams";
  private static String VIEWS = "views";

  private static String JOBCLASS = "jobclass";
  private static String NAME = "name";
  private static String PROPS = "props";
  private static String URI_PROP = "uri";

  public static StreamDescription parseJson (String json) {

    ObjectMapper mapper = new ObjectMapper();

    JsonNode parent;

    try {
      parent = mapper.readValue(json, JsonNode.class);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new AppException(e);
    }

    StreamDescription.Builder builder = new StreamDescription.Builder();

    String className = parent.get(JOBCLASS).asText();

    try {
      Class jobClass = Thread.currentThread().getContextClassLoader().loadClass(className);

      builder.jobClass(jobClass);

    } catch (ClassNotFoundException e) {
      throw new AppException(e);
    }

    // Read the streams.
    ArrayNode streams = (ArrayNode) parent.get(STREAMS);

    for (JsonNode stream: streams) {

      String name = stream.get(NAME).asText();

      ObjectNode props = (ObjectNode) stream.get(PROPS);

      Map<String,String> properties = Maps.newHashMap();

      for (Iterator<Map.Entry<String,JsonNode>> it =  props.fields(); it.hasNext();) {

        Map.Entry<String, JsonNode> property = it.next();

        properties.put(property.getKey(), property.getValue().asText());
      }

      builder.withStream(name, properties);
    }

    // Read the views.
    ArrayNode views = (ArrayNode) parent.get(VIEWS);

    for (JsonNode view: views) {

      String name = view.get(NAME).asText();
      String uri = view.get(URI_PROP).asText();

      builder.withView(name, uri);
    }

    return builder.build();
  }

  private TreeNode toJson() {

    JsonNodeFactory js = JsonNodeFactory.instance;

    ObjectNode root = js.objectNode();

    root.put(JOBCLASS, getJobClass().getName());

    ArrayNode streamsArray = js.arrayNode();

    for (Stream stream: getStreams().values()) {

      ObjectNode streamNode = streamsArray.addObject();

      streamNode.put(NAME, stream.getName());

      ObjectNode properties = js.objectNode();

      for(Map.Entry<String,String> prop: stream.getProperties().entrySet()) {

        properties.put(prop.getKey(), prop.getValue());
      }

      streamNode.put(PROPS, properties);
    }

    root.put(STREAMS, streamsArray);


    ArrayNode viewsArray = js.arrayNode();

    for (Map.Entry<String,URI> view: getViewUris().entrySet()) {

      ObjectNode viewNode = viewsArray.addObject();

      viewNode.put(NAME, view.getKey());
      viewNode.put(URI_PROP, view.getValue().toString());
    }

    root.put(VIEWS, viewsArray);

    return root;
  }

  public String toString() {

    StringWriter writer = new StringWriter();

    try {

      JsonGenerator gen = new JsonFactory().createGenerator(writer);
      gen.setCodec(new ObjectMapper());
      gen.writeTree(toJson());

      gen.close();
    } catch (IOException e) {
      // An IOException should not be possible against a local buffer.
      throw new AssertionError(e);
    }

    return writer.toString();
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }

    if (that == null || !(that instanceof StreamDescription)) {
      return false;
    }

    StreamDescription _that = (StreamDescription) that;

    return jobName.equals(_that.jobName) &&
        jobClass.equals(_that.jobClass) &&
        streams.equals(_that.streams) &&
        viewUris.equals(_that.viewUris);
  }

  @Override
  public int hashCode() {

    return 37 * jobClass.getName().hashCode() *
        streams.hashCode() * viewUris.hashCode();

  }
}
