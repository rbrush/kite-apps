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
import java.util.Random;

/**
 * Deploys the Kite application to a Hadoop cluster.
 */
public class AppDeployer {

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

    // Install to a temporary destination and rename it to avoid
    // potential races against other installers.
    String tempBase = conf.get("hadoop.tmp.dir", "/tmp");
    Path tempDestination =  new Path(tempBase, "kite-" + (new Random().nextInt() & Integer.MAX_VALUE));

    Application app;

    try {
      app = applicationClass.newInstance();

    } catch (Exception e) {
      throw new AppException("Unable to create an instance of the app: "
          + applicationClass, e);
    }

    app.setup(conf);

    try {

      fs.mkdirs(tempDestination);

    } catch (IOException e) {
      throw new AppException(e);
    }

    List<Schedule> schedules = app.getSchedules();

    for (Schedule schedule: schedules) {

      installWorkflow(tempDestination, schedule);

      installCoordinator(tempDestination, schedule);
    }

    installBundle(applicationClass, tempDestination, appPath, schedules);

    installJars(new Path(tempDestination, "lib"), jars);

    try {
      if (!fs.rename(tempDestination, appPath)) {

        throw new AppException("Unable to rename " +
            tempDestination + " to " + appPath + ".");
      }

    } catch (IOException e) {
      throw new AppException(e);
    }
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
  private void installWorkflow(Path appPath, Schedule schedule) {

    Path workflowPath = new Path (appPath, OozieScheduling.workflowPath(schedule));

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
  }


  private void installCoordinator(Path appPath, Schedule schedule) {

    SchedulableJobManager manager = JobManagers.create(schedule.getJobClass(), conf);

    Path coordDirectory = new Path (appPath, OozieScheduling.coordPath(schedule));

    Path coordPath = new Path(coordDirectory, "coordinator.xml");

    OutputStream outputStream = null;

    try {

      fs.mkdirs(coordDirectory);

      outputStream = fs.create(coordPath);

      OozieScheduling.writeCoordinator(schedule, manager, outputStream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {

      if (outputStream != null)
        Closeables.closeQuietly(outputStream);
    }
  }

  private Path installBundle(Class appClass, Path tempDestination, Path appPath, List<Schedule> schedules) {

    Path bundlePath = new Path(tempDestination, "oozie/bundle.xml");

    OutputStream outputStream = null;

    try {

      outputStream = fs.create(bundlePath);

      OozieScheduling.writeBundle(appClass, conf, appPath, schedules, outputStream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {

      if (outputStream != null)
        Closeables.closeQuietly(outputStream);
    }
    return bundlePath;
  }
}


