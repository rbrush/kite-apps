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
package org.kitsdk.apps.spark.spi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.plexus.util.xml.XMLWriter;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.spi.oozie.OozieScheduling;
import org.kitesdk.apps.spi.oozie.ShareLibs;
import org.kitesdk.data.View;
import org.kitsdk.apps.spark.AbstractSchedulableSparkJob;

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


  public static SparkJobManager create(Class<? extends AbstractSchedulableSparkJob> jobClass,
                                       Configuration conf) {

    AbstractSchedulableSparkJob job;

    try {
      job = jobClass.newInstance();
    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }

    job.setConf(conf);

    job.setContext(DefaultSparkContext.getContext());

    Method runMethod = resolveRunMethod(job);

    return new SparkJobManager(job, runMethod, conf);
  }

  SparkJobManager(SchedulableJob job, Method runMethod, Configuration conf) {
    super(job, runMethod, conf);
  }

  @Override
  public void run(Instant nominalTime, Map<String,View> views) {

    // Use the default context if provided.
    JavaSparkContext context = DefaultSparkContext.getContext();

    boolean localContext = false;

    // If no default spark context exists, create one for this job.
    if (context == null) {

      localContext = true;
      SparkConf conf = new SparkConf().setAppName(job.getName());

      context = new JavaSparkContext(conf);
    }

    try {

      job.setNominalTime(nominalTime);

      job.setConf(getConf());
      ((AbstractSchedulableSparkJob) job).setContext(context);

      Object[] args = getArgs(views);

      runMethod.invoke(job, args);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    } catch (InvocationTargetException e) {
      throw new AppException(e);
    } finally {
      // If we created our own context, stop it.
      if (localContext)
        context.stop();
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

    OozieScheduling.writeJobConfiguration(writer, schedule, getConf());

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

    writer.endElement(); // spark
  }

  private  final String getSparkConfString(Schedule schedule) {

    // Pass the job settings as Hadoop settings to be used by the underlying
    // system.
    Map<String, String> settings = OozieScheduling.getJobSettings(schedule, getConf());

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
      List<Path> jars = ShareLibs.jars(getConf(), "hive2");

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

    return Arrays.asList(jarFiles);
  }

}



