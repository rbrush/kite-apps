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
package org.kitesdk.apps.spi;


import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import junit.framework.Assert;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.oozie.XMLUtil;
import org.kitesdk.apps.test.apps.ScheduledInputOutputApp;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.w3c.dom.Document;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class AppDeployerTest extends MiniDFSTest {

  FileSystem fs;

  @Before
  public void setup() throws IOException {

    this.fs = getDFS();

    // Create a test direcotry in HDFS.


    DefaultConfiguration.set(getConfiguration());
  }

  private Path createTestDirectory() {
    String tempBase = getConfiguration().get("hadoop.tmp.dir", "/tmp");

    String tempName = "testapp-" + (new Random().nextInt() & Integer.MAX_VALUE);
    return fs.makeQualified(new Path(tempBase, tempName));
  }

  private static File createTestProperties() throws IOException {

    File propFile = File.createTempFile("test", "properties");

    Properties props = new Properties();

    props.put("key1", "val1");
    props.put("key2", "val2");

    FileOutputStream output = new FileOutputStream(propFile);

    try {

      props.store(output, "");

    } finally {
      output.close();
    }

    return propFile;
  }

  @Test
  public void testDeploySimpleApp() throws Exception {

    Path testDirectory = createTestDirectory();

    AppDeployer deployer = new AppDeployer(fs, new AppContext(getConfiguration()));

    ScheduledInputOutputApp app = new ScheduledInputOutputApp();

    app.setup(new AppContext(getConfiguration()));

    File propFile = createTestProperties();

    deployer.install(app, testDirectory, propFile, Collections.<File>emptyList());

    assertValidInstallation(testDirectory);
  }

  /**
   * Asserts that the application installed at thet given path is a valid
   * structure.
   */
  private void assertValidInstallation(Path appPath) throws Exception {

    Path workflowDir = new Path(appPath, "oozie/workflows");
    Path coordDir = new Path(appPath, "oozie/coordinators");
    Path bundleFile = new Path(appPath, "oozie/bundle.xml");
    Path confFile = new Path(appPath, "conf/app.properties");
    Path schedule = new Path(appPath, "schedules/test-job.json");

    Assert.assertTrue(fs.exists(appPath));
    Assert.assertTrue(fs.exists(workflowDir));
    Assert.assertTrue(fs.exists(coordDir));
    Assert.assertTrue(fs.exists(bundleFile));
    Assert.assertTrue(fs.exists(confFile));
    Assert.assertTrue(fs.exists(schedule));

    validatePropertyFile(fs, confFile);

    validateSchedule(fs, schedule);

    // Make sure the expected application path is set.
    InputStream input = fs.open(bundleFile);

    try {

      Document dom = XMLUtil.toDom(fs.open(bundleFile));

      XPath xpath = XMLUtil.getXPath();

      // Make sure our root is written to the exptected test directory.
      String appRoot = xpath.evaluate("bn:bundle-app/bn:parameters/bn:property" +
          "/bn:value[../bn:name/text() = \"kiteAppRoot\"]", dom);

      Assert.assertTrue(appRoot.startsWith(appPath.toString()));

    } finally {

      input.close();
    }

    boolean foundWorkflow = false;

    // Ensure all workflow files are valid.
    for (FileStatus status: fs.listStatus(workflowDir)) {

      foundWorkflow = true;

      Path workflowFile = new Path(status.getPath(), "workflow.xml");

      validateSchema(fs, workflowFile);
    }

    Assert.assertTrue("No test workflow found.", foundWorkflow);

    boolean foundCoordinator = false;

    // Ensure all coordinator files are valid.
    for (FileStatus status: fs.listStatus(coordDir)) {

      foundCoordinator = true;

      Path coordinatorFile = new Path(status.getPath(), "coordinator.xml");

      validateSchema(fs, coordinatorFile);
    }

    Assert.assertTrue("No test coordinator found.", foundCoordinator);

    validateSchema(fs, bundleFile);
  }

  private void validatePropertyFile(FileSystem fs, Path path) {

    InputStream input = null;

    try {
      input = fs.open(path);

      Properties props = new Properties();

      props.load(input);

      Assert.assertTrue(props.stringPropertyNames().size() > 0);

    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      Closeables.closeQuietly(input);
    }
  }

  private void validateSchedule(FileSystem fs, Path path) {

    InputStream input = null;

    try {
      input = fs.open(path);

      String json = CharStreams.toString(new InputStreamReader(input));


      Schedule schedule = Schedule.parseJson(json);

      Assert.assertEquals("test-job", schedule.getName());

    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      Closeables.closeQuietly(input);
    }
  }


  /**
   * Validates the file written at the given path is a valid Oozie bundle, coordinator
   * or workflow.
   */
  public static void validateSchema(FileSystem fs, Path path) {

    try {

      // Get Oozie schemas from the oozie-client JAR.
      StreamSource[] sources = new StreamSource[] {
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-workflow-0.5.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-coordinator-0.4.xsd")),
          new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("oozie-bundle-0.2.xsd"))};

      SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
      Schema schema = factory.newSchema(sources);
      Validator validator = schema.newValidator();

      InputStream input = fs.open(path);
      try {
        validator.validate(new StreamSource(input));
      } finally {

        input.close();
      }

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
