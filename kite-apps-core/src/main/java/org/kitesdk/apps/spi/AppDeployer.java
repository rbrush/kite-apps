package org.kitesdk.apps.spi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.Application;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.spi.oozie.OozieScheduling;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Deploys the Kite application to a Hadoop cluster.
 */
public class AppDeployer {

  public static final String WORKFLOW_DIR = "oozie/workflows";

  public static final String COORD_DIR = "oozie/coordinators";

  private final FileSystem fs;

  private final Configuration conf;


  public AppDeployer(FileSystem fs, Configuration conf) {
    this.fs = fs;
    this.conf = conf;
  }

  /**
   * Deploys the application to the cluster.
   */
  public void deploy(Class<? extends Application> applicationClass, Path appPath, List<File> jars) {
    install(applicationClass, appPath, jars);

    // TODO: get the oozie URL from arguments?
    String oozieURL = System.getenv("OOZIE_URL");

    if (oozieURL == null) {
      throw new AppException("No OOZIE_URL environment variable specified");
    }

    OozieClient client =  new OozieClient(oozieURL);

    start(client, appPath);
  }

  @VisibleForTesting
  void install(Class<? extends Application> applicationClass, Path appPath, List<File> jars) {

    Application app;

    try {
      app = applicationClass.newInstance();

    } catch (Exception e) {
      throw new AppException("Unable to create an instance of the app: "
          + applicationClass, e);
    }

    app.setup(conf);

    try {

      fs.mkdirs(appPath);

    } catch (IOException e) {
      throw new AppException(e);
    }

    List<Schedule> schedules = app.getSchedules();

    Map<String,Path> coordinatorPaths = Maps.newHashMap();

    for (Schedule schedule: schedules) {

      Path workflowPath = installWorkflow(appPath, schedule);

      Path coordinatorPath = installCoordinator(appPath, workflowPath, schedule);

      coordinatorPaths.put(schedule.getName(), coordinatorPath);
    }

    installBundle(applicationClass, appPath, coordinatorPaths);

    installJars(OozieScheduling.libPath(appPath), jars);
  }


  private Configuration filterConfig(Configuration conf) {

    Configuration appConfig = new Configuration(conf);

    // Ugly way of including Hive settings to be visible in
    // the application configuration. We should find a better way.
    appConfig.addResource("hive-site.xml");

    // Remove properties disallowed by Oozie.
    // TODO: better way to do this? Are these defined somewhere?

    if (appConfig.get("mapred.job.tracker") != null)
      appConfig.unset("mapred.job.tracker");

    if (appConfig.get("fs.default.name") != null)
      appConfig.unset("fs.default.name");

    return appConfig;
  }

  private void installJars(Path libPath, List<File> jars) {

    try {
      fs.mkdirs(libPath);

      for (File jarFile: jars) {
        fs.copyFromLocalFile(new Path(jarFile.getAbsolutePath()), new Path(libPath, jarFile.getName()));
      }

    } catch (IOException e) {
      throw new AppException(e);
    }
  }

  /**
   * Starts the application in Oozie.
   */
  private String start(OozieClient oozieClient, Path appPath) {

    Properties props = oozieClient.createConfiguration();

    Path bundlePath = new Path(appPath, "oozie/bundle.xml");

    props.setProperty(OozieClient.BUNDLE_APP_PATH, bundlePath.toString());
    props.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");

    try {
      return oozieClient.run(props);

    } catch (OozieClientException e) {
      throw new AppException(e);
    }
  }


  /**
   * Installs a workflow for the given schedule under the application path
   * and return the location of the installed workflow.
   */
  private Path installWorkflow(Path appPath, Schedule schedule) {

    Path workflowPath = new Path (appPath, WORKFLOW_DIR + "/" + schedule.getName());

    Path workflowXMLPath = new Path(workflowPath, "workflow.xml");

    OutputStream outputStream = null;

    try {

      outputStream = fs.create(workflowXMLPath);

      OozieScheduling.writeWorkFlow(schedule, conf, outputStream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {

      if (outputStream != null)
        Closeables.closeQuietly(outputStream);
    }

    return workflowPath;
  }


  private Path installCoordinator(Path appPath, Path workflowPath, Schedule schedule) {

    SchedulableJobManager manager = JobManagers.create(schedule.getJobClass(), conf);

    Path coordDirectory = new Path (appPath, COORD_DIR + "/" + schedule.getName());

    Path coordPath = new Path(coordDirectory, "coordinator.xml");

    OutputStream outputStream = null;

    try {

      fs.mkdirs(coordDirectory);

      outputStream = fs.create(coordPath);

      OozieScheduling.writeCoordinator(schedule, manager, workflowPath, outputStream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {

      if (outputStream != null)
        Closeables.closeQuietly(outputStream);
    }

    return coordPath;
  }

  private Path installBundle(Class appClass, Path appPath, Map<String,Path> coordinatorPaths) {

    Path bundlePath = new Path(appPath, "oozie/bundle.xml");

    OutputStream outputStream = null;

    try {

      outputStream = fs.create(bundlePath);

      OozieScheduling.writeBundle(appClass, conf, appPath, coordinatorPaths, outputStream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {

      if (outputStream != null)
        Closeables.closeQuietly(outputStream);
    }
    return bundlePath;
  }
}


