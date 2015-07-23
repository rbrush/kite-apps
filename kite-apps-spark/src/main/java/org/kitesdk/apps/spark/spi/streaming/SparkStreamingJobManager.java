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
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


public class SparkStreamingJobManager implements StreamingJobManager<AbstractStreamingSparkJob> {

  private final StreamDescription description;

  private final AbstractStreamingSparkJob job;

  private final Method runMethod;

  private final SparkJobContext sparkJobContext;

  private final AppContext appContext;

  public SparkStreamingJobManager(StreamDescription description,
                                  AbstractStreamingSparkJob job, Method runMethod, AppContext context) {

    this.description = description;
    this.job = job;
    this.runMethod = runMethod;
    this.appContext = context;
    this.sparkJobContext = new SparkJobContext(job.getName(), context);
  }

  public static Path jobDescriptionFile(Path appRoot, String jobName) {
    return new Path (appRoot, "streaming/" + jobName + ".json");
  }


  public static StreamDescription loadDescription(FileSystem fs, Path appRoot, String jobName) {

    Path streamingJobPath = jobDescriptionFile(appRoot, jobName);

    StringBuilder builder = new StringBuilder();
    InputStream input = null;

    try {
      input = fs.open(streamingJobPath);

      InputStreamReader streamReader = new InputStreamReader(input);

      BufferedReader reader = new BufferedReader(streamReader);



      String line;

      while ((line = reader.readLine()) != null) {

        builder.append(line);
      }
    } catch(IOException e) {
      throw new AppException(e);
    } finally {
      Closeables.closeQuietly(input);
    }

    return StreamDescription.parseJson(builder.toString());
  }

  private static void writeDescription(FileSystem fs, Path appRoot, StreamDescription description) {

    Path streamingJobPath = jobDescriptionFile(appRoot, description.getJobName());

    try {
      fs.mkdirs(streamingJobPath.getParent());
    } catch (IOException e) {
      throw new AppException(e);
    }

    OutputStream output = null;

    try {
      output = fs.append(streamingJobPath);
      OutputStreamWriter writer = new OutputStreamWriter(output);
      writer.write(description.toString());

    } catch (IOException e) {
      throw new AppException(e);
    } finally {
      Closeables.closeQuietly(output);
    }
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
  public void install(FileSystem fs, Path appRoot) {

    Path descriptionFile = jobDescriptionFile(appRoot, job.getName());

    try {

      OutputStream output = fs.create(descriptionFile);

      try {

        OutputStreamWriter writer = new OutputStreamWriter(output);

        writer.append(description.toString());
        writer.flush();

      } finally {
        output.close();
      }

    } catch (IOException e) {
      throw new AppException(e);
    }
  }

  @Override
  public void start(FileSystem fs, Path appRoot) {
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(SparkStreamingJobMain.class);
    String containingJar = jobConf.getJar();

    Path libPath = new Path(appRoot, "lib");

    Path jarPath = new Path(libPath, new File(containingJar).getName());
    jarPath = fs.makeQualified(jarPath);

    SparkLauncher launcher = new SparkLauncher();

    launcher.setMainClass(SparkStreamingJobMain.class.getName());

    launcher.setAppResource(jarPath.toString());

    launcher.setMaster("yarn-cluster");

    // Add the library JARs from HDFS so we don't need to reload
    // them separately into Spark.
    try {
      FileStatus[] libJars = fs.listStatus(libPath);

      for (FileStatus jar: libJars) {

        launcher.addJar(jar.getPath().toString());
      }

    } catch (IOException e) {
      throw new AppException(e);
    }

    launcher.addAppArgs(appRoot.toString(),
        description.getJobName());

    try {

      Process process = launcher.launch();

      // Redirect the spark-submit output to be visible to the reader.
      Thread stdoutThread = writeOutput(process.getInputStream(), System.out);
      Thread stderrThread = writeOutput(process.getErrorStream(), System.err);

      int result = process.waitFor();

      stdoutThread.join();
      stderrThread.join();

      if (result != 0) {
        throw new AppException("spark-submit returned error status: " + result);
      }

    } catch (IOException e) {
      throw new AppException(e);
    } catch (InterruptedException e) {
      throw new AppException(e);
    }
  }

  /**
   * Writes the output to std out.
   */
  private static Thread writeOutput(final InputStream stream, final PrintStream target) {

    Thread thread = new Thread("spark-submit-output-redirect") {

      public void run() {

        Scanner scanner = new Scanner(stream);

        while (scanner.hasNextLine()) {
          target.println(scanner.nextLine());
        }
      }
    };

    thread.setDaemon(true);
    thread.start();

    return thread;
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
   * Run the job in the local process. This is generally used for unit tests.
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
