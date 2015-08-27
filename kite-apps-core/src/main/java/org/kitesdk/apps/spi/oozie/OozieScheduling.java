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
package org.kitesdk.apps.spi.oozie;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.JobParameters;
import org.kitesdk.apps.scheduled.Schedule;

import org.codehaus.plexus.util.WriterFactory;
import org.codehaus.plexus.util.xml.PrettyPrintXMLWriter;
import org.codehaus.plexus.util.xml.XMLWriter;
import org.codehaus.plexus.util.xml.XmlStreamWriter;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Support for creating and using Oozie workflows.
 */
public class OozieScheduling {

  static final String OOZIE_COORD_NS = "uri:oozie:coordinator:0.4";

  static final String OOZIE_WORKFLOW_NS = "uri:oozie:workflow:0.5";

  static final String OOZIE_BUNDLE_NS = "uri:oozie:bundle:0.2";

  static final String OOZIE_SPARK_NS = "uri:oozie:spark-action:0.1";

  private static final String WORKFLOW_ELEMENT = "workflow-app";

  private static final String COORDINATOR_ELEMENT = "coordinator-app";

  private static final String BUNDLE_ELEMENT = "bundle-app";

  private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm'Z'")
      .withZone(DateTimeZone.UTC);

  private static final String COORD_NOMINAL_TIME = "coordNominalTime";

  private static final String WORKFLOW_NOMINAL_TIME = "workflowNominalTime";
  private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

  /**
   * Relative path to where Oozie workflows are stored.
   */
  private static final String WORKFLOW_DIR = "oozie/workflows";

  /**
   * Relative path to where Oozie coordinators are stored.
   */
  private static final String COORD_DIR = "oozie/coordinators";


  /**
   * Returns the relative path for the workflow generated for the schedule.
   */
  public static String workflowPath(Schedule schedule) {

    return WORKFLOW_DIR + "/" + schedule.getName();
  }

  /**
   * Returns the relative path for the coordinator generated for the schedule.
   */
  public static String coordPath(Schedule schedule) {

    return COORD_DIR + "/" + schedule.getName();
  }

  public static Instant getNominalTime(Configuration conf) {

    String timeString = conf.get(WORKFLOW_NOMINAL_TIME);

    if (timeString == null) {
      return null;
    }

    return new Instant(formatter.parseMillis(timeString));
  }

  public static void element(XMLWriter writer, String name, String attributeName,
                              String attributeValue) {

    writer.startElement(name);
    writer.addAttribute(attributeName, attributeValue);
    writer.endElement();
  }

  public static void element(XMLWriter writer, String name, String text) {

    writer.startElement(name);
    writer.writeText(text);
    writer.endElement();
  }

  /**
   * Generates an Oozie workflow to execute the job in the given schedule.
   *
   * @param schedule the schedule to write
   * @param context the application's context
   * @param output an output stream to which the workflow is written
   */
  public static void writeWorkFlow(Schedule schedule,
                                   AppContext context,
                                   OutputStream output) throws IOException {

    XmlStreamWriter streamWriter = WriterFactory.newXmlWriter(output);

    PrettyPrintXMLWriter writer = new PrettyPrintXMLWriter(streamWriter);

    writer.startElement(WORKFLOW_ELEMENT);
    writer.addAttribute("name", schedule.getName());
    writer.addAttribute("xmlns", OOZIE_WORKFLOW_NS);

    element(writer, "start", "to", schedule.getName());

    writer.startElement("action");
    writer.addAttribute("name", schedule.getName());

    // Reduce retry attempts. TODO: make this configurable?
    writer.addAttribute("retry-max", "2");
    writer.addAttribute("retry-interval", "1");

    // Write the appropriate action to be used in the job.
    SchedulableJobManager manager = JobManagers.createSchedulable(schedule.getJobClass(), schedule.getName(), context);
    manager.writeOozieActionBlock(writer, schedule);

    element(writer, "ok", "to", "end");

    element(writer, "error", "to", "kill");

    writer.endElement(); // action

    writer.startElement("kill");
    writer.addAttribute("name", "kill");
    element(writer, "message", "Error in workflow for " + schedule.getName());
    writer.endElement(); // kill

    element(writer, "end", "name", "end");

    writer.endElement(); // workflow
    streamWriter.flush();
  }

  private static final String toIdentifier(String name) {

    return name.replace(".", "_");
  }

  /**
   * Qualifies the given URI template with metastore URI information
   * from the given configuration, if it doesn't exist.
   */
  @VisibleForTesting
  static final String qualifyUri(Configuration conf, String uriTemplate) {

    // Create a URI that doesn't include the initial view: or dataset: prefix
    // and excludes the parameters.
    URI baseURI = URI.create(uriTemplate.substring(uriTemplate.indexOf(':') + 1, uriTemplate.indexOf('?')));

    // We only need to qualify hive URIs that
    // are not already qualified with a host,
    // when there is a Hive metastore URI configured.
    if (!"hive".equals(baseURI.getScheme()) ||
        baseURI.getHost() != null ||
        conf.get(HIVE_METASTORE_URIS) == null) {

      return uriTemplate;
    }

    String[] uris = conf.get(HIVE_METASTORE_URIS).split(",");

    if (uris.length == 0) {
      return uriTemplate;
    }

    URI hiveUri = URI.create(uris[0]);

    String host = hiveUri.getHost();
    int port = hiveUri.getPort();

    // Recreate a template this is qualified with host and port information.
    String prefix = uriTemplate.startsWith("dataset") ? "dataset:hive:" :
        "view:hive:";

    String rest = uriTemplate.substring(prefix.length());

    return prefix + "//" + host + ":" + port + "/" + rest;
  }

  private static final void writeCoordinatorDatasets(XMLWriter writer,
                                                     Schedule schedule,
                                                     SchedulableJobManager manager) {

    writer.startElement("datasets");

    for (Map.Entry<String,Schedule.ViewTemplate> entry: schedule.getViewTemplates().entrySet()) {


      Schedule.ViewTemplate template = entry.getValue();

      writer.startElement("dataset");
      writer.addAttribute("name", "ds_" + toIdentifier(entry.getKey()));

      // Write the frequency in Oozie's pre-cron format. This should be removed
      // when See https://issues.apache.org/jira/browse/OOZIE-1431 is available.
      writer.addAttribute("frequency", CronConverter.toFrequency(template.getFrequency()));

      Instant initialTime = CronConverter.nextInstant(template.getFrequency(), new Instant());

      writer.addAttribute("initial-instance", formatter.print(initialTime));

      writer.addAttribute("timezone", "UTC");

      String qualifiedTemplate = qualifyUri(manager.getAppContext().getHadoopConf(),
          entry.getValue().getUriTemplate());

      element(writer, "uri-template", qualifiedTemplate);

      // Don't createSchedulable a done flag. This may be something we can remove when
      // when using a URI handler aware of Kite.
      element(writer, "done-flag", "");
      writer.endElement(); // dataset
    }

    writer.endElement(); // datasets

    Set<String> inputNames = manager.getJobParameters().getInputNames();

    if (!inputNames.isEmpty()) {
      writer.startElement("input-events");

      for (String inputName: inputNames) {
        writer.startElement("data-in");
        writer.addAttribute("name", "datain_" + toIdentifier(inputName));
        writer.addAttribute("dataset", "ds_" + toIdentifier(inputName));
        element(writer, "instance", "${coord:current(0)}");
        writer.endElement(); // data-in
      }

      writer.endElement(); // input-events
    }

    Set<String> outputNames = manager.getJobParameters().getOutputNames();


    if (!outputNames.isEmpty()) {
      writer.startElement("output-events");

      for (String outputName: outputNames) {
        writer.startElement("data-out");
        writer.addAttribute("name", "dataout_" + toIdentifier(outputName));
        writer.addAttribute("dataset", "ds_" + toIdentifier(outputName));
        element(writer, "instance", "${coord:current(0)}");
        writer.endElement(); // data-out
      }

      writer.endElement(); // output-events
    }
  }

  /**
   * Generates an Oozie coordinator XML for the given schedule.
   *
   * @param schedule the schedule for which a coordinator is to be written
   * @param manager the manager instance fot the scheduled job
   * @param output an output stream to which the generated schedule is written
   */
  public static void writeCoordinator(Schedule schedule,
                                      SchedulableJobManager manager,
                                      OutputStream output) throws IOException {

    XmlStreamWriter streamWriter = WriterFactory.newXmlWriter(output);

    PrettyPrintXMLWriter writer = new PrettyPrintXMLWriter(streamWriter);

    String jobName = schedule.getJobClass().getCanonicalName();

    writer.startElement(COORDINATOR_ELEMENT);
    writer.addAttribute("name", jobName);
    writer.addAttribute("xmlns", OOZIE_COORD_NS);
    writer.addAttribute("frequency", schedule.getFrequency());
    writer.addAttribute("start", formatter.print(schedule.getStartTime()));

    writer.addAttribute("end", "3000-01-01T00:00Z");

    writer.addAttribute("timezone", "UTC");

    writeCoordinatorDatasets(writer, schedule, manager);

    writer.startElement("action");

    writer.startElement("workflow");
    element(writer, "app-path", "${kiteAppRoot}/" + workflowPath(schedule));

    writer.startElement("configuration");

    property(writer, "kiteAppRoot", "${kiteAppRoot}");

    property(writer, COORD_NOMINAL_TIME, "${coord:nominalTime()}");

    // Include the dataset inputs to make them visible to the workflow.
    for (String inputName: manager.getJobParameters().getInputNames()) {

      property(writer, "coord_" + toIdentifier(inputName),
          "${coord:dataIn('datain_" + toIdentifier(inputName) + "')}");
    }

    for (String outputName: manager.getJobParameters().getOutputNames()) {

      property(writer, "coord_" + toIdentifier(outputName),
          "${coord:dataOut('dataout_" + toIdentifier(outputName) + "')}");
    }

    writer.endElement(); // configuration

    writer.endElement(); // workflow
    writer.endElement(); // action
    writer.endElement(); // coordinator
    streamWriter.flush();
  }

  /**
   * Writes a Hadoop-style configuration property.
   */
  public static void property(XMLWriter writer, String name, String value) {
    writer.startElement("property");

    element(writer, "name", name);
    element(writer, "value", value);

    writer.endElement();
  }


  public static void writeBundle(Class appClass,
                                 AppContext context,
                                 Path appPath,
                                 List<Schedule> schedules,
                                 OutputStream output) throws IOException {

    Configuration conf = context.getHadoopConf();

    XmlStreamWriter streamWriter = WriterFactory.newXmlWriter(output);

    PrettyPrintXMLWriter writer = new PrettyPrintXMLWriter(streamWriter);

    writer.startElement(BUNDLE_ELEMENT);
    writer.addAttribute("name", appClass.getCanonicalName());
    writer.addAttribute("xmlns", OOZIE_BUNDLE_NS);

    writer.startElement("parameters");

    // Default to the HDFS scheme for the root path if none is provided.
    Path qualifiedPath = appPath.toUri().getScheme() == null ?
        appPath.makeQualified(URI.create("hdfs:/"), appPath) :
        appPath;

    property(writer, "kiteAppRoot", qualifiedPath.toString());

    property(writer, "oozie.libpath", "${kiteAppRoot}/lib");
    property(writer, "nameNode", conf.get("fs.default.name"));

    String resourceManager = conf.get("yarn.resourcemanager.address");

    // MR2 uses YARN for the job tracker, but some Hadoop deployments
    // don't have the resoure manager setting visible. We work around this
    // by grabbing the job tracker setting and swapping to the resource
    // manager port.
    // TODO: is there a better way to deal with this?
    if (resourceManager == null) {

      String jobTracker = conf.get("mapred.job.tracker");

      if (jobTracker != null)
        resourceManager = jobTracker.replace("8021", "8032");
    }

    if (resourceManager != null)
      property(writer, "jobTracker", resourceManager);

    // TODO: handle application configuration.
//    if (appConfigPath != null)
//     property(writer, "appConfigPath", appConfigPath.toString());
    writer.endElement(); // parameters

    int i = 0;

    for (Schedule schedule: schedules) {
      writer.startElement("coordinator");
      writer.addAttribute("name", schedule.getName());

      element(writer, "app-path", "${kiteAppRoot}/" + coordPath(schedule));
      writer.endElement(); // coordinator
    }

    writer.endElement(); // bundle
    streamWriter.flush();
  }

  /**
   * Loads the Kite views to be passed to a job at runtime.
   *
   * @return A map of named inputs to the corresponding views
   */
  public static Map<String,View> loadViews(SchedulableJobManager manager, Configuration conf) {

    Map<String,View> views = Maps.newHashMap();

    JobParameters params = manager.getJobParameters();

    for (String inputName: params.getInputNames()) {

      String uri = conf.get("wf_" + OozieScheduling.toIdentifier(inputName));

      views.put(inputName,
          Datasets.load(uri, params.getRecordType(inputName)));
    }

    for (String outputName: params.getOutputNames()) {

      String uri = conf.get("wf_" + OozieScheduling.toIdentifier(outputName));

      views.put(outputName,
          Datasets.load(uri, params.getRecordType(outputName)));
    }

    return views;
  }

  /**
   * Returns the settings to be passed to a job runner.
   */
  public static Map<String,String> getJobSettings(Schedule schedule, Configuration conf) {

    Map<String,String> settings = Maps.newHashMap();

    settings.put(WORKFLOW_NOMINAL_TIME, "${" + COORD_NOMINAL_TIME + "}");

    // Write the Hive metastore URI, if available. It may be null
    // in local or testing environments.
    if (conf.get(HIVE_METASTORE_URIS) != null) {
      settings.put(HIVE_METASTORE_URIS, conf.get(HIVE_METASTORE_URIS));
    }

    // Include the dataset inputs to make them visible to the workflow.
    for (String name: schedule.getViewTemplates().keySet()) {

      settings.put("wf_" + toIdentifier(name), "${coord_" + toIdentifier(name) + "}");
    }

    return settings;
  }

  public static void writeJobConfiguration(XMLWriter writer, Schedule schedule, Configuration conf) {

    property(writer, WORKFLOW_NOMINAL_TIME, "${" + COORD_NOMINAL_TIME + "}");

    // Write the Hive metastore URI, if available. It may be null
    // in local or testing environments.
    if (conf.get(HIVE_METASTORE_URIS) != null) {
      property(writer, HIVE_METASTORE_URIS, conf.get(HIVE_METASTORE_URIS));
    }

    // Include the dataset inputs to make them visible to the workflow.
    for (String name: schedule.getViewTemplates().keySet()) {

      property(writer, "wf_" + toIdentifier(name), "${coord_" + toIdentifier(name) + "}");
    }
  }
}