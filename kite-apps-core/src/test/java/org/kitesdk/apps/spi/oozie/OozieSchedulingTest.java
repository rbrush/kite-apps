package org.kitesdk.apps.spi.oozie;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.test.apps.AltScheduledInputOutputJob;
import org.kitesdk.apps.test.apps.ScheduledInputOutputApp;
import org.kitesdk.apps.test.apps.ScheduledInputOutputJob;

import org.w3c.dom.Document;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;;

import static org.junit.Assert.assertEquals;

public class OozieSchedulingTest  {

  Schedule testSchedule;

  @Before
  public void createTestSchedule() {

    testSchedule = new Schedule.Builder().jobClass(ScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .withInput("source.users", ScheduledInputOutputApp.INPUT_URI_PATTERN, "0 * * * *")
        .withOutput("target.users", ScheduledInputOutputApp.OUTPUT_URI_PATTERN)
        .build();
  }


  @Test
  public void testWriteWorkflow() throws Exception {

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    OozieScheduling.writeWorkFlow(testSchedule, new Configuration(), output);

    Document workflow = XMLUtil.toDom(output);

    XPath xpath = XMLUtil.getXPath();

    assertEquals(testSchedule.getName(),
        xpath.evaluate("wf:workflow-app/@name", workflow));

    // The oozie main should be invoked with the argument specifying the job.
    assertEquals(OozieScheduledJobMain.class.getName(),
        xpath.evaluate("wf:workflow-app/wf:action/wf:java/wf:main-class", workflow));

    assertEquals(ScheduledInputOutputJob.class.getName(),
        xpath.evaluate("wf:workflow-app/wf:action/wf:java/wf:arg", workflow));

    // Check that the nominal time and source and target datasets are passed
    // as configuration to the Oozie job.
    assertEquals("${coordNominalTime}",
        xpath.evaluate("wf:workflow-app/wf:action/wf:java/wf:configuration/" +
            "wf:property/wf:value[../wf:name/text() = \"workflowNominalTime\"]", workflow));

    assertEquals("${coord_source_users}",
        xpath.evaluate("wf:workflow-app/wf:action/wf:java/wf:configuration/" +
            "wf:property/wf:value[../wf:name/text() = \"wf_source_users\"]", workflow));

    assertEquals("${coord_target_users}",
        xpath.evaluate("wf:workflow-app/wf:action/wf:java/wf:configuration/" +
            "wf:property/wf:value[../wf:name/text() = \"wf_target_users\"]", workflow));
  }

  @Test
  public void testWriteCoordinator() throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    SchedulableJobManager manager = JobManagers.create(ScheduledInputOutputJob.class,
        new Configuration());

    OozieScheduling.writeCoordinator(testSchedule, manager, output);

    Document coord = XMLUtil.toDom(output);

    XPath xpath = XMLUtil.getXPath();

    assertEquals(testSchedule.getFrequency(),
        xpath.evaluate("coord:coordinator-app/@frequency", coord));

    // check for the input and output datasets
    assertEquals(xpath.evaluate("coord:coordinator-app/coord:datasets/" +
            "coord:dataset[@name = \"ds_source_users\"]/coord:uri-template", coord),
        ScheduledInputOutputApp.INPUT_URI_PATTERN);

    assertEquals(xpath.evaluate("coord:coordinator-app/coord:datasets/" +
            "coord:dataset[@name = \"ds_target_users\"]/coord:uri-template", coord),
        ScheduledInputOutputApp.OUTPUT_URI_PATTERN);

    assertEquals("${coord:current(0)}",
        xpath.evaluate("coord:coordinator-app/coord:input-events/coord:data-in/coord:instance",
            coord));

    assertEquals("${coord:current(0)}",
        xpath.evaluate("coord:coordinator-app/coord:output-events/coord:data-out/coord:instance",
            coord));

    assertEquals("${kiteAppRoot}/oozie/workflows/scheduled-input-output",
        xpath.evaluate("coord:coordinator-app/coord:action/coord:workflow/coord:app-path", coord));

    // Check the nominal time is set for the workflow.
    assertEquals("${coord:nominalTime()}",
        xpath.evaluate("coord:coordinator-app/coord:action/coord:workflow/coord:configuration/" +
            "coord:property/coord:value[../coord:name/text() = \"coordNominalTime\"]", coord));

    // Check data input and output properties are set for the workflow.
    assertEquals("${coord:dataIn('datain_source_users')}",
        xpath.evaluate("coord:coordinator-app/coord:action/coord:workflow/coord:configuration/" +
            "coord:property/coord:value[../coord:name/text() = \"coord_source_users\"]", coord));

    assertEquals("${coord:dataOut('dataout_target_users')}",
        xpath.evaluate("coord:coordinator-app/coord:action/coord:workflow/coord:configuration/" +
            "coord:property/coord:value[../coord:name/text() = \"coord_target_users\"]", coord));
  }

  @Test
  public void testWriteBundle() throws Exception {

    String appPath = "/test/app/path";
    String libPath = "/test/lib/path";


    Schedule schedule1 = new Schedule.Builder()
        .jobClass(ScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .withInput("source.users", ScheduledInputOutputApp.INPUT_URI_PATTERN, "0 * * * *")
        .withOutput("target.users", ScheduledInputOutputApp.OUTPUT_URI_PATTERN)
        .build();

    Schedule schedule2 = new Schedule.Builder()
        .jobClass(AltScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .withInput("source.users", ScheduledInputOutputApp.INPUT_URI_PATTERN, "0 * * * *")
        .withOutput("target.users", ScheduledInputOutputApp.OUTPUT_URI_PATTERN)
        .build();

    List<Schedule> schedules = Arrays.asList(schedule1, schedule2);

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    OozieScheduling.writeBundle(ScheduledInputOutputApp.class, new Configuration(),
        new Path(appPath), schedules, output);

    Document bundle = XMLUtil.toDom(output);

    XPath xpath = XMLUtil.getXPath();

    // Check expected coordinator names.
    assertEquals(new ScheduledInputOutputJob().getName(),
        xpath.evaluate("bn:bundle-app/bn:coordinator[1]/@name", bundle));

    assertEquals(new AltScheduledInputOutputJob().getName(),
        xpath.evaluate("bn:bundle-app/bn:coordinator[2]/@name", bundle));

    // Entries for the coordinator paths should exist.
    assertEquals("${kiteAppRoot}/" + OozieScheduling.coordPath(schedule1),
        xpath.evaluate("bn:bundle-app/bn:coordinator[1]/bn:app-path", bundle));

    assertEquals("${kiteAppRoot}/" + OozieScheduling.coordPath(schedule2),
        xpath.evaluate("bn:bundle-app/bn:coordinator[2]/bn:app-path", bundle));
  }
}
