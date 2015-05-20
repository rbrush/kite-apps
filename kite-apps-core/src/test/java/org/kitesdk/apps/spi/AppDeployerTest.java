package org.kitesdk.apps.spi;


import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.test.apps.ScheduledInputOutputApp;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DefaultConfiguration;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

public class AppDeployerTest extends MiniDFSTest {

  Path testDirectory;

  FileSystem fs;

  @Before
  public void setup() throws IOException {
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.fs = getDFS();
    DefaultConfiguration.set(getConfiguration());
  }

  @Test
  public void testDeploySimpleApp() throws IOException {

    AppDeployer deployer = new AppDeployer(fs, getConfiguration());
    deployer.install(ScheduledInputOutputApp.class, testDirectory, Collections.<File>emptyList());

    assertValidInstallation(testDirectory);
  }

  /**
   * Asserts that the application installed at thet given path is a valid
   * structure.
   */
  private void assertValidInstallation(Path appPath) throws IOException {

    Path workflowDir = new Path(appPath, AppDeployer.WORKFLOW_DIR);
    Path coordDir = new Path(appPath, AppDeployer.COORD_DIR);
    Path bundleFile = new Path(appPath, "oozie/bundle.xml");

    Assert.assertTrue(fs.exists(appPath));
    Assert.assertTrue(fs.exists(workflowDir));
    Assert.assertTrue(fs.exists(coordDir));
    Assert.assertTrue(fs.exists(bundleFile));

    // Ensure all workflow files are valid.
    for (FileStatus status: fs.listStatus(workflowDir)) {

      Path workflowFile = new Path(status.getPath(), "workflow.xml");

      validateSchema(fs, workflowFile);
    }

    // Ensure all coordinator files are valid.
    for (FileStatus status: fs.listStatus(coordDir)) {

      Path coordinatorFile = new Path(status.getPath(), "coordinator.xml");

      validateSchema(fs, coordinatorFile);
    }

    validateSchema(fs, bundleFile);
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
