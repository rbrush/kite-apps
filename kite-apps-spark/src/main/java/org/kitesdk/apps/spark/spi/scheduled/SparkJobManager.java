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
package org.kitesdk.apps.spark.spi.scheduled;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.plexus.util.xml.XMLWriter;
import org.joda.time.Instant;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spark.SparkJobContext;
import org.kitesdk.apps.spi.jobs.JobReflection;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.spi.oozie.OozieScheduling;
import org.kitesdk.apps.spi.oozie.ShareLibs;
import org.kitesdk.data.View;
import org.kitesdk.apps.spark.AbstractSchedulableSparkJob;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.kitesdk.apps.spi.oozie.OozieScheduling.element;
import static org.kitesdk.apps.spi.oozie.OozieScheduling.property;

/**
 * Spark job manager.
 */
class SparkJobManager extends SchedulableJobManager {

  private volatile SparkJobContext sparkJobContext;


  public static SparkJobManager create(Class<? extends AbstractSchedulableSparkJob> jobClass,
                                       String jobName,
                                       AppContext context) {

    AbstractSchedulableSparkJob job;

    try {
      job = jobClass.newInstance();
    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }

    return new SparkJobManager(job, jobName, context);
  }

  SparkJobManager(SchedulableJob job, String jobName, AppContext context) {
    super(job, jobName, context);

  }

  @Override
  public JobContext getJobContext() {

    if (sparkJobContext == null) {
      sparkJobContext = new SparkJobContext(jobName, context);
    }

    return sparkJobContext;
  }

  @Override
  public void run(Instant nominalTime, Map<String,View> views) {


    try {

      Method runMethod = JobReflection.resolveRunMethod(job.getClass());

      job.setNominalTime(nominalTime);
      job.setJobContext(getJobContext());

      Object[] args = JobReflection.getArgs(runMethod, views);

      runMethod.invoke(job, args);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    } catch (InvocationTargetException e) {
      throw new AppException(e);
    } finally {
    }

    signalOutputViews(views);
  }

  @Override
  public void writeOozieActionBlock(XMLWriter writer, Schedule schedule) {

    writer.startElement("spark");
    writer.addAttribute("xmlns", "uri:oozie:spark-action:0.1");
    element(writer, "job-tracker", "${jobTracker}");
    element(writer, "name-node", "${nameNode}");

    // TODO: the job-xml should probably be job-specific configuration.
    // element(writer, "job-xml", "${appConfigPath}");

    // Make the nominal time visible to the workflow action.
    writer.startElement("configuration");

    // Use the spark and hive sharelibs since many actions use both.
    property(writer, "oozie.action.sharelib.for.spark", "spark,hive2");
    property(writer, "kiteAppRoot", "${kiteAppRoot}");

    OozieScheduling.writeJobConfiguration(writer, schedule, context.getHadoopConf());

    writer.endElement(); // configuration

    element(writer, "master", "yarn-cluster");
    element(writer, "name", schedule.getName());
    element(writer, "class", SparkScheduledJobMain.class.getCanonicalName());

    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(schedule.getJobClass());
    String containingJar = jobConf.getJar();

    String jarName = containingJar != null ?
        "${kiteAppRoot}/lib/" + new File(containingJar).getName() :
        "";

    element(writer, "jar",  jarName);
    element(writer, "spark-opts", getSparkConfString(schedule));
    element(writer, "arg", schedule.getJobClass().getName());
    element(writer, "arg", schedule.getName());

    writer.endElement(); // spark
  }

  private  final String getSparkConfString(Schedule schedule) {

    // Pass the job settings as Hadoop settings to be used by the underlying
    // system.
    Map<String, String> settings = OozieScheduling.getJobSettings(schedule, context.getHadoopConf());

    StringBuilder builder = new StringBuilder();

    for(Map.Entry<String,String> setting: settings.entrySet()) {

      builder.append("--conf ")
          .append("spark.hadoop.")
          .append(setting.getKey())
          .append("=")
          .append(setting.getValue())
          .append(" ");
    }

    // Include the comma-separated JARs to be used.
    builder.append("--jars ")
        .append(getJarString(schedule.getJobClass()));

    return builder.toString();
  }

  private final String getJarString(Class jobClass) {

    List<File> libJars = getLibraryJars();

    StringBuilder builder = new StringBuilder();

    boolean first = true;

    for (File jarFile: libJars) {
      if (jarFile.getName().endsWith("jar")) {

        if (!first)
          builder.append(",");

        builder.append("${kiteAppRoot}/lib/");
        builder.append(jarFile.getName());

        first = false;
      }
    }

    try {

      // Add sharelib JARs explicitly to the action as a workaround
      // for https://issues.apache.org/jira/browse/OOZIE-2277

      // No need to add the Spark shared library here because
      // it is automatically included in the spark action.
      List<Path> jars = ShareLibs.jars(context.getHadoopConf(), "hive2");

      for (Path jar: jars) {

        builder.append(",");
        builder.append(jar.toString());
      }

    } catch (IOException e) {

      throw new AppException(e);
    }

    return builder.toString();
  }

  private static final List<File> getLibraryJars() {

    // Current implementation assumes that library files
    // are in the same directory, so locate it and
    // include it in the project library.

    // This is ugly, using the jobConf logic to identify the containing
    // JAR. There should be a better way to do this.
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(SchedulableJob.class);
    String containingJar = jobConf.getJar();

    if (containingJar == null)
      return Collections.emptyList();

    File file = new File(containingJar).getParentFile();

    File[] jarFiles = file.listFiles();

    if (jarFiles == null)
      throw new AppException("Unable to list jar files at " + file.getAbsolutePath());

    return Arrays.asList(jarFiles);
  }

}



