package org.kitesdk.apps.spi.oozie;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.SchedulableJobManager;
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
import java.util.Iterator;
import java.util.List;;

import static org.junit.Assert.assertEquals;

public class OozieSchedulingTest  {

  Schedule testSchedule;

  @Before
  public void createTestSchedule() {

    testSchedule = new Schedule.Builder().jobClass(ScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .withView("source.users", ScheduledInputOutputApp.INPUT_URI_PATTERN, 1)
        .withView("target.users", ScheduledInputOutputApp.OUTPUT_URI_PATTERN, 1)
        .build();
  }

  private Document toDom(ByteArrayOutputStream output) throws Exception {

    // TODO: validate as part of document builder.
    InputStream input = new ByteArrayInputStream(output.toByteArray());

    try {
      // Get Oozie schemas from the oozie-client JAR.
      StreamSource[] sources = new StreamSource[] {
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-workflow-0.5.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-coordinator-0.4.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-bundle-0.2.xsd"))};

      SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
      Schema schema = factory.newSchema(sources);
      Validator validator = schema.newValidator();

      try {
        validator.validate(new StreamSource(input));
      } finally {

        input.close();
      }

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    DocumentBuilderFactory docBuilderFactory
        = DocumentBuilderFactory.newInstance();
    //ignore all comments inside the xml file
    docBuilderFactory.setIgnoringComments(true);

    //allow includes in the xml file
    docBuilderFactory.setNamespaceAware(true);
    try {
      docBuilderFactory.setXIncludeAware(true);
    } catch (UnsupportedOperationException e) {

    }

    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();

    return builder.parse(new ByteArrayInputStream(output.toByteArray()));
  }

  private static final NamespaceContext CONTEXT = new NamespaceContext() {

    public String getNamespaceURI(String prefix) {

      if ("wf".equals(prefix))
        return OozieScheduling.OOZIE_WORKFLOW_NS;

      if ("coord".equals(prefix))
        return OozieScheduling.OOZIE_COORD_NS;

      if ("bn".equals(prefix))
        return OozieScheduling.OOZIE_BUNDLE_NS;

      throw new IllegalArgumentException("Unknown prefix:" + prefix);
    }

    public Iterator getPrefixes(String val) {
      return null;
    }

    public String getPrefix(String uri) {

      if (OozieScheduling.OOZIE_WORKFLOW_NS.equals(uri))
        return "wf";

      if (OozieScheduling.OOZIE_COORD_NS.equals(uri))
        return "coord";

      if (OozieScheduling.OOZIE_BUNDLE_NS.equals(uri))
        return "bn";

      throw new IllegalArgumentException("Unknown uri:" + uri);
    }
  };

  @Test
  public void testWriteWorkflow() throws Exception {

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    OozieScheduling.writeWorkFlow(testSchedule, new Configuration(), output);

    Document workflow = toDom(output);

    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(CONTEXT);

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

    String workFlowPath = "/test/workflow/path";

    SchedulableJobManager manager = SchedulableJobManager.create(ScheduledInputOutputJob.class,
        new Configuration());

    OozieScheduling.writeCoordinator(testSchedule, manager, new Path(workFlowPath), output);

    Document coord = toDom(output);

    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(CONTEXT);

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

    assertEquals(workFlowPath,
        xpath.evaluate("coord:coordinator-app/coord:action/coord:workflow/coord:app-path", coord));
  }

  @Test
  public void testWriteBundle() throws Exception {

    String appPath = "/test/app/path";
    String libPath = "/test/lib/path";

    String coordPath1 = "/some/coord/path/1";
    String coordPath2 = "/some/coord/path/2";

    List<Path> coordinatorPaths = Lists.newArrayList();

    coordinatorPaths.add(new Path(coordPath1));
    coordinatorPaths.add(new Path(coordPath2));

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    OozieScheduling.writeBundle(ScheduledInputOutputApp.class, new Configuration(), new Path(appPath),
        new Path(libPath), coordinatorPaths, output);

    Document bundle = toDom(output);

    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(CONTEXT);

    // Entries for the coordinator paths should exist.
    assertEquals(coordPath1,
        xpath.evaluate("bn:bundle-app/bn:coordinator[1]/bn:app-path", bundle));

    assertEquals(coordPath2,
        xpath.evaluate("bn:bundle-app/bn:coordinator[2]/bn:app-path", bundle));
  }
}
