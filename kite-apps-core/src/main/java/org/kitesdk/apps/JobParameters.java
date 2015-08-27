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
package org.kitesdk.apps;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * The input and output parameters to be used when launching a job.
 * Most users will not need to create this directly, since they can use
 * {@link org.kitesdk.apps.scheduled.AbstractSchedulableJob} or
 *
 */
public class JobParameters {

  private final Set<String> inputNames;

  private final Set<String> outputNames;

  private final Map<String,Schema> schemas;

  private final Map<String,Class> recordTypes;

  private final Map<String,Class> paramTypes;

  private JobParameters(Set<String> inputNames,
                        Set<String> outputNames,
                        Map<String,Schema> schemas,
                        Map<String,Class> recordTypes,
                        Map<String,Class> paramTypes) {

    this.inputNames = Collections.unmodifiableSet(inputNames);
    this.outputNames = Collections.unmodifiableSet(outputNames);
    this.schemas = Collections.unmodifiableMap(schemas);
    this.recordTypes = Collections.unmodifiableMap(recordTypes);
    this.paramTypes = Collections.unmodifiableMap(paramTypes);
  }

  public Set<String> getInputNames() {
    return inputNames;
  }

  public Set<String> getOutputNames() {
    return outputNames;
  }

  public Schema getSchema(String parameterName) {
    return schemas.get(parameterName);
  }

  public Class getRecordType(String parameterName) {
    return recordTypes.get(parameterName);
  }

  public Class getParameterType(String parameterName) {
    return paramTypes.get(parameterName);
  }

  /**
   * Builder to create a JobParameters instance.
   */
  public static class Builder {

    private boolean built = false;

    private final Set<String> inputNames = Sets.newHashSet();

    private final Set<String> outputNames = Sets.newHashSet();

    private final Map<String,Schema> schemas = Maps.newHashMap();

    private final Map<String,Class> recordTypes = Maps.newHashMap();

    private final Map<String,Class> paramTypes = Maps.newHashMap();

    private void checkBuilt() {
      if (built)
        throw new IllegalStateException("Job parameters has already been built.");
    }

    public void input(String name, Schema schema, Class recordType, Class paramType) {

      inputNames.add(name);

      if (schema != null)
        schemas.put(name, schema);

      recordTypes.put(name, recordType);
      paramTypes.put(name, paramType);
    }

    public void output(String name, Schema schema, Class recordType, Class paramType) {

      outputNames.add(name);

      if (schema != null)
        schemas.put(name, schema);

      recordTypes.put(name, recordType);
      paramTypes.put(name, paramType);
    }

    public JobParameters build() {

      checkBuilt();
      built = true;

      return new JobParameters(inputNames, outputNames, schemas, recordTypes, paramTypes);
    }
  }
}
