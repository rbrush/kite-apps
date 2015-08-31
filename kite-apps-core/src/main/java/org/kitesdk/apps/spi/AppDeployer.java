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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.Application;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.jobs.JobManagers;
import org.kitesdk.apps.spi.jobs.SchedulableJobManager;
import org.kitesdk.apps.spi.jobs.StreamingJobManager;
import org.kitesdk.apps.spi.oozie.OozieScheduling;
import org.kitesdk.apps.streaming.StreamDescription;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Deploys the Kite application to a Hadoop cluster.
 */
public class AppDeployer {

  private final FileSystem fs;

  private final AppContext context;

  private final Random random;

  public AppDeployer(FileSystem fs, AppContext context) {
    this.fs = fs;
    this.context = context;
    this.random = new Random();
  }


  public void deploy(Class<? extends Application> applicationClass, Path appPath, File settingsFile, List<File> jars)  {

    Application app;

    try {
      app = applicationClass.newInstance();

    } catch (Exception e) {
      throw new AppException("Unable to createSchedulable an instance of the app: "
          + applicationClass, e);
    }


    // TODO: get new app context... based on settings...

    app.setup(context);

    install(app, appPath, settingsFile, jars);

    // Start scheduled apps with Oozie, if there are any.
    if (!app.getSchedules().isEmpty()) {
      // TODO: get the oozie URL from arguments?
      String oozieURL = System.getenv("OOZIE_URL");

      if (oozieURL == null) {
        throw new AppException("No OOZIE_URL environment variable specified");
      }

      OozieClient client =  new OozieClient(oozieURL);

      start(client, appPath);
    }

    // Start the streaming jobs.
    for (StreamDescription description: app.getStreamDescriptions()) {

      StreamingJobManager manager =  JobManagers.createStreaming(description, context);

      manager.start(fs, appPath);
    }
  }

  @VisibleForTesting
  public void install(Application app, Path appPath, File settingsFile, List<File> jars) {

    // Install to a temporary destination and rename it to avoid
    // potential races against other installers.
    String tempBase = context.getHadoopConf().get("hadoop.tmp.dir", "/tmp");
    Path tempDestination =  new Path(tempBase, "kite-" + (random.nextInt() & Integer.MAX_VALUE));

    try {

      fs.mkdirs(tempDestination);

    } catch (IOException e) {
      throw new AppException(e);
    }

    if (settingsFile != null) {

      // Install the configuration.
      Path appProps = new Path(tempDestination, "conf/app.properties");

      try {

        fs.copyFromLocalFile(new Path(settingsFile.getAbsolutePath()), appProps);
      } catch (IOException e) {
        throw new AppException(e);
      }
    }


    // Install the scheduled jobs.
    List<Schedule> schedules = app.getSchedules();

    for (Schedule schedule: schedules) {

      writeSchedule(tempDestination, schedule);

      installWorkflow(tempDestination, schedule);

      installCoordinator(tempDestination, schedule);
    }

    installBundle(app.getClass(), tempDestination, appPath, schedules);

    // Install the streaming jobs.
    List<StreamDescription> descriptions = app.getStreamDescriptions();

    for (StreamDescription description: descriptions) {

      StreamingJobManager manager = JobManagers.createStreaming(description, context);

      manager.install(fs, tempDestination);
    }

    // Install the library JARs.
    installJars(new Path(tempDestination, "lib"), jars);

    // Copy the temporary path to the final location.
    try {
      if (!fs.rename(tempDestination, appPath)) {

        throw new AppException("Unable to rename " +
            tempDestination + " to " + appPath + ".");
      }

    } catch (IOException e) {
      throw new AppException(e);
    }
  }

  private void writeSchedule(Path tempDestination, Schedule schedule)  {

    Path scheduleFile = SchedulableJobManager.scheduleFile(tempDestination, schedule.getName());

    Writer writer = null;

    try {

      fs.mkdirs(scheduleFile.getParent());

      FSDataOutputStream output = fs.create(scheduleFile);

      writer = new OutputStreamWriter(output, Charsets.UTF_8);

      writer.write(schedule.toString());

    } catch (IOException e) {

      throw new AppException(e);

    } finally {

      Closeables.closeQuietly(writer);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="UPM_UNCALLED_PRIVATE_METHOD", justification="Future work")
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

      OozieScheduling.writeWorkFlow(schedule, context, outputStream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {

      if (outputStream != null)
        Closeables.closeQuietly(outputStream);
    }
  }


  private void installCoordinator(Path appPath, Schedule schedule) {

    SchedulableJobManager manager = JobManagers.createSchedulable(schedule, context);

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

      OozieScheduling.writeBundle(appClass, context, appPath, schedules, outputStream);

    } catch (IOException e) {
      throw new AppException(e);
    } finally {

      if (outputStream != null)
        Closeables.closeQuietly(outputStream);
    }
    return bundlePath;
  }
}


