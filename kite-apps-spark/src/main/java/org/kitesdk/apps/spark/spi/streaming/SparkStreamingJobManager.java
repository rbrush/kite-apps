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
package org.kitesdk.apps.spark.spi.streaming;

import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.apps.spark.AbstractStreamingSparkJob;
import org.kitesdk.apps.spark.SparkJobContext;
import org.kitesdk.apps.spi.jobs.JobUtil;
import org.kitesdk.apps.spi.jobs.StreamingJobManager;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.apps.streaming.StreamingJob;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;
import org.kitesdk.spark.backport.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class SparkStreamingJobManager implements StreamingJobManager<AbstractStreamingSparkJob> {

  private final StreamDescription description;

  private final AbstractStreamingSparkJob job;

  private final Method runMethod;

  private final SparkJobContext sparkJobContext;

  public SparkStreamingJobManager(StreamDescription description,
                                  AbstractStreamingSparkJob job, Method runMethod, AppContext context) {

    this.description = description;
    this.job = job;
    this.runMethod = runMethod;
    this.sparkJobContext = new SparkJobContext(job.getName(), context);
  }

  public static SparkStreamingJobManager create(StreamDescription description,
                                                AppContext context) {

    AbstractStreamingSparkJob job;

    try {
      job = (AbstractStreamingSparkJob) description.getJobClass().newInstance();
    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }

    Method runMethod = JobUtil.resolveRunMethod(job);

    return new SparkStreamingJobManager(description, job, runMethod, context);
  }

  private static final List<File> getLibraryJars() {

    // Current implementation assumes that library files
    // are in the same directory, so locate it and
    // include it in the project library.

    // This is ugly, using the jobConf logic to identify the containing
    // JAR. There should be a better way to do this.
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(StreamingJob.class);
    String containingJar = jobConf.getJar();

    if (containingJar == null)
      return Collections.emptyList();

    File file = new File(containingJar).getParentFile();

    File[] jarFiles = file.listFiles();

    return Arrays.asList(jarFiles);
  }

  @Override
  public void deploy() {

    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(job.getClass());
    String containingJar = jobConf.getJar();

    String jarName = containingJar != null ?
        "${kiteAppRoot}/lib/" + new File(containingJar).getName() :
        "";
    SparkLauncher launcher = new SparkLauncher();

    launcher.setMainClass(SparkStreamingJobMain.class.getName());

    launcher.setAppResource(jarName);

    List<File> jarFiles = getLibraryJars();

    for (File file: jarFiles) {
      launcher.addJar(file.getAbsolutePath());
    }

    // TODO: add path to app and root?
    // Pass in the broker, zookeeper, and job connection information.
    launcher.addAppArgs();

    try {

      Process process = launcher.launch();

      int result = process.waitFor();

      if (result != 0) {
        throw new AppException("spark-submit returned error status: " + result);
      }

    } catch (IOException e) {
      throw new AppException(e);
    } catch (InterruptedException e) {
      throw new AppException(e);
    }
  }


  private static boolean isStream(Class sourceType) {

    return JavaDStream.class.isAssignableFrom(sourceType);
  }

  private JavaDStream load(StreamDescription description, DataIn input) {

    StreamDescription.Stream stream = description.getStreams().get(input.name());

    // Currently on Kafka is the only stream type supported.
    // Future enhancements may determine a different loader based
    // on properties provided by the caller.
    SparkKafkaStreamLoader loader = new SparkKafkaStreamLoader();

    if (input.type() == null) {
      throw new AppException("Job " + description.getJobClass().getName() +
      " must specify a type for input " + input.name());
    }

    if (SpecificRecord.class.isAssignableFrom(input.type())) {

      Schema schema = SpecificData.get().getSchema(input.type());

      return loader.load(schema, stream.getProperties(), sparkJobContext);

    } else {
      throw new UnsupportedOperationException("Current implementation only supports specific types in streams.");
    }
  }

  /**
   * Run the job in the local process.
   */
  public void run()  {

    Map<String, Class> sourceTypes = JobUtil.getTypes(runMethod);

    Map<String,Object> parameters = Maps.newHashMap();

    for(DataIn input: JobUtil.getInputs(runMethod).values()) {

      if (isStream(sourceTypes.get(input.name()))) {

        JavaDStream stream = load(description, input);

        parameters.put(input.name(), stream);

      } else {

        throw new UnsupportedOperationException("non-stream inputs are not yet supported in stream operations.");
      }
    }

    for (DataOut output: JobUtil.getOutputs(runMethod).values()) {

      if (isStream(sourceTypes.get(output.name()))) {

        throw new UnsupportedOperationException("stream outputs not yet supported.");

      } else {

        URI uri = description.getViewUris().get(output.name());

        if (uri == null)
          throw new AppException("No URI defined for output: " + output.name());

        View view = Datasets.load(uri);

        parameters.put(output.name(), view);
      }
    }

    Object[] args = JobUtil.getArgs(runMethod, parameters);

    job.setJobContext(sparkJobContext);

    // Run the job itself.
    try {
      runMethod.invoke(job, args);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    } catch (InvocationTargetException e) {
      throw new AppException(e);
    }
  }
}
