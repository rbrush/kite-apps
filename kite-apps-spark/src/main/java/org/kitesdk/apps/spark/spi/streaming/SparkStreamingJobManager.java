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

import com.google.common.base.Charsets;
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
import org.kitesdk.apps.DataIn;
import org.kitesdk.apps.DataOut;
import org.kitesdk.apps.JobParameters;
import org.kitesdk.apps.spark.AbstractStreamingSparkJob;
import org.kitesdk.apps.spark.SparkJobContext;
import org.kitesdk.apps.spark.kafka.KafkaOutput;
import org.kitesdk.apps.spark.spi.kryo.KryoAvroRegistrator;
import org.kitesdk.apps.spi.jobs.JobReflection;
import org.kitesdk.apps.spi.jobs.StreamingJobManager;
import org.kitesdk.apps.spi.oozie.ShareLibs;
import org.kitesdk.apps.streaming.StreamDescription;
import org.kitesdk.apps.streaming.StreamingJob;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;
import org.kitesdk.spark.backport.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SparkStreamingJobManager implements StreamingJobManager<AbstractStreamingSparkJob> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkStreamingJobManager.class);

  private final StreamDescription description;

  private final AbstractStreamingSparkJob job;

  private final SparkJobContext sparkJobContext;

  private final AppContext appContext;

  public SparkStreamingJobManager(StreamDescription description,
                                  AbstractStreamingSparkJob job, AppContext context) {

    this.description = description;
    this.job = job;
    this.appContext = context;
    this.sparkJobContext = new SparkJobContext(description, context);
  }

  public static Path jobDescriptionFile(Path appRoot, String jobName) {
    return new Path (appRoot, "streaming/" + jobName + ".json");
  }

  public static StreamDescription loadDescription(FileSystem fs, Path appRoot, String jobName)  {

    Path streamingJobPath = jobDescriptionFile(appRoot, jobName);

    StringBuilder builder = new StringBuilder();
    try {
      InputStream input = fs.open(streamingJobPath);

      InputStreamReader streamReader = new InputStreamReader(input, Charsets.UTF_8);

      BufferedReader reader = new BufferedReader(streamReader);

      try {

        String line;

        while ((line = reader.readLine()) != null) {

          builder.append(line);
        }
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new AppException(e);
    }

    return StreamDescription.parseJson(builder.toString());
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

    return new SparkStreamingJobManager(description, job, context);
  }

  @Override
  public void install(FileSystem fs, Path appRoot) {

    Path descriptionFile = jobDescriptionFile(appRoot, description.getJobName());

    try {

      OutputStream output = fs.create(descriptionFile);
      OutputStreamWriter writer = new OutputStreamWriter(output, Charsets.UTF_8);

      try {

        writer.append(description.toString());

      } finally {
        writer.close();
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


    try {
      // Add the library JARs from HDFS so we don't need to reload
      // them separately into Spark.
      FileStatus[] libJars = fs.listStatus(libPath);

      for (FileStatus jar: libJars) {

        launcher.addJar(jar.getPath().toString());
      }

      // Add the sharelib JARs, since they are not visible to Spark otherwise.
      List<Path> shareLibJars = ShareLibs.jars(sparkJobContext.getHadoopConf(), "hive2");

      for (Path sharelibJar: shareLibJars) {

        launcher.addJar(fs.makeQualified(sharelibJar).toString());
      }

    } catch (IOException e) {
      throw new AppException(e);
    }

    launcher.addAppArgs(appRoot.toString(),
        description.getJobName());

    // Explicitly set the metastore URI to be usable in the job.
    launcher.setConf("spark.hadoop.hive.metastore.uris",
        sparkJobContext.getHadoopConf().get("hive.metastore.uris"));

    // Add provided Spark arguments to the job.
    for (Map.Entry<String,String> setting: sparkJobContext.getSettings().entrySet()) {

      if (setting.getKey().startsWith("spark.")) {

        launcher.setConf(setting.getKey(), setting.getValue());
      }
    }

    // Add the Avro classes.
    List<Schema> schemas = JobReflection.getSchemas(job);
    StringBuilder avroClassesArg = new StringBuilder();

    avroClassesArg
        .append("-D")
        .append(KryoAvroRegistrator.KITE_AVRO_CLASSES)
        .append("=");

    boolean first = true;

    for (Schema schema: schemas) {

      if (!first) {
        avroClassesArg.append(",");
      }

      avroClassesArg.append(SpecificData.get().getClass(schema).getName());

      first = false;
    }

    launcher.setConf("spark.driver.extraJavaOptions", avroClassesArg.toString());
    launcher.setConf("spark.executor.extraJavaOptions", avroClassesArg.toString());

    try {

      Process process = launcher.launch();

      AtomicBoolean isRunning = new AtomicBoolean(false);

      // Redirect the spark-submit output to be visible to the reader. We will
      // return when we detect that the job is in a running state.
      Thread stdoutThread = redirectOutput(process.getInputStream(), isRunning);
      Thread stderrThread = redirectError(process.getErrorStream(), isRunning);

      stdoutThread.join();
      stderrThread.join();

      if (!isRunning.get()) {
        throw new AppException("spark-submit failed; see YARN logs for details");
      }

    } catch (IOException e) {
      throw new AppException(e);
    } catch (InterruptedException e) {
      throw new AppException(e);
    }
  }

  private static final Pattern RUNNING_PATTERN = Pattern.compile(".*RUNNING.*");

  /**
   * Redirect output to stdout and set isRunning to true when the underlying
   * process is running.
   */
  private static Thread redirectError(final InputStream stream, final AtomicBoolean isRunning) {

    Thread thread = new Thread("spark-submit-output-redirect") {

      public void run() {

        Scanner scanner = new Scanner(stream, Charsets.UTF_8.name());

        try {
          while (scanner.hasNextLine() && !isRunning.get()) {

            String line = scanner.nextLine();

            // The launcher program runs indefinitely, so we look for
            // a running status to indicate the application is launched.
            Matcher matcher = RUNNING_PATTERN.matcher(line);

            if (matcher.matches()) {
              isRunning.set(true);
            }

            System.err.println(line);
          }

        } finally {
          scanner.close();
        }
      }
    };

    thread.setDaemon(true);
    thread.start();

    return thread;
  }

  private static Thread redirectOutput(final InputStream input, final AtomicBoolean isRunning) {

    Thread thread = new Thread("spark-submit-stderr-redirect") {

      public void run() {

        try {

          while (!isRunning.get()) {

            if (input.available() > 0) {

              int value = input.read();
              System.out.write(value);

            } else {

              // No data, so sleep briefly to avoid CPU saturation.
              Thread.sleep(500);
            }
          }

        } catch (InterruptedException e) {
          LOGGER.warn("Exception redirecting stdout", e);
        } catch (IOException e) {
          LOGGER.warn("Exception redirecting stdout", e);
        }
      }
    };

    thread.setDaemon(true);
    thread.start();

    return thread;
  }


  private static boolean isStream(Class sourceType) {

    // DStream and Kafka outputs can be currently used to
    // stream data.
    return JavaDStream.class.isAssignableFrom(sourceType) ||
        KafkaOutput.class.isAssignableFrom(sourceType);
  }

  private JavaDStream load(Map<String,String> inputSettings, StreamDescription description, String inputName, Class recordType) {

    // Currently on Kafka is the only stream type supported.
    // Future enhancements may determine a different loader based
    // on properties provided by the caller.
    SparkKafkaStreamLoader loader = new SparkKafkaStreamLoader();


    if (SpecificRecord.class.isAssignableFrom(recordType)) {

      Schema schema = SpecificData.get().getSchema(recordType);

      return loader.load(schema, inputSettings, sparkJobContext);

    } else {
      throw new UnsupportedOperationException("Current implementation only supports specific types in streams.");
    }
  }

  /**
   * Run the job in the local process. This is generally used for unit tests.
   */
  public void run()  {

    Map<String,Object> parameters = Maps.newHashMap();

    JobParameters jobParams = job.getParameters();

    for(String inputName: jobParams.getInputNames()) {

      if (isStream(jobParams.getParameterType(inputName))) {

        Map<String,String> inputSettings = sparkJobContext.getInputSettings(inputName);

        Class inputType = jobParams.getRecordType(inputName);

        JavaDStream stream = load(inputSettings, description, inputName, inputType);

        parameters.put(inputName, stream);

      } else {

        throw new UnsupportedOperationException("non-stream inputs are not yet supported in stream operations.");
      }
    }

    for (String outputName: jobParams.getOutputNames()) {

      if (isStream(jobParams.getParameterType(outputName))) {

        Map<String,String> outputSettings = sparkJobContext.getOutputSettings(outputName);

        Schema schema = SpecificData.get().getSchema(jobParams.getRecordType(outputName));

        parameters.put(outputName, new KafkaOutput(schema, outputSettings));

      } else {

        URI uri = description.getViewUris().get(outputName);

        if (uri == null)
          throw new AppException("No URI defined for output: " + outputName);

        View view = Datasets.load(uri);

        parameters.put(outputName, view);
      }
    }

    job.runJob(parameters, sparkJobContext);
  }
}
