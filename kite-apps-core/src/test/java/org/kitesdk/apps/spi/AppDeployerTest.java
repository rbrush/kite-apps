package org.kitesdk.apps.spi;


import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.apps.spi.AppDeployer;
import org.kitesdk.apps.spi.oozie.OozieTestUtils;
import org.kitesdk.apps.spi.oozie.TestApp;
import org.kitesdk.data.MiniDFSTest;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class AppDeployerTest extends MiniDFSTest {

  Path testDirectory;

  FileSystem fs;

  @Before
  public void setup() throws IOException {
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.fs = getDFS();
  }

  @Test
  public void testDeploySimpleApp() throws IOException {

    AppDeployer deployer = new AppDeployer(fs, getConfiguration());
    deployer.install(TestApp.class, testDirectory, Collections.<File>emptyList());

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

      OozieTestUtils.validateSchema(fs, workflowFile);
    }

    // Ensure all coordinator files are valid.
    for (FileStatus status: fs.listStatus(coordDir)) {

      Path coordinatorFile = new Path(status.getPath(), "coordinator.xml");

      OozieTestUtils.validateSchema(fs, coordinatorFile);
    }

    OozieTestUtils.validateSchema(fs, bundleFile);
  }
}
