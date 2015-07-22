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
package org.kitesdk.apps.spi.jobs;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.plexus.util.xml.XMLWriter;
import org.codehaus.plexus.util.xml.XmlStreamWriter;
import org.joda.time.Instant;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.JobContext;
import org.kitesdk.apps.scheduled.SchedulableJob;
import org.kitesdk.apps.scheduled.Schedule;
import org.kitesdk.apps.spi.oozie.OozieScheduledJobMain;
import org.kitesdk.apps.spi.oozie.OozieScheduling;
import org.kitesdk.data.View;

import javax.xml.stream.XMLStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static org.kitesdk.apps.spi.oozie.OozieScheduling.element;
import static org.kitesdk.apps.spi.oozie.OozieScheduling.property;

/**
 * Supports creating and scheduling jobs as Oozie Java actions.
 */
class JavaActionJobManager extends SchedulableJobManager {


  public JavaActionJobManager(SchedulableJob job, Method runMethod, AppContext context) {
    super(job, runMethod, context);
  }

  @Override
  public JobContext getJobContext() {
    Map<String, String> settings = JobUtil.toJobSettings(job.getName(), context);
    Configuration conf = JobUtil.toJobHadoopConf(job.getName(), context);

    return new JobContext(settings, conf);
  }

  public static JavaActionJobManager create(Class<? extends SchedulableJob> jobClass,
                                            AppContext context) {

    SchedulableJob job;

    try {
      job = jobClass.newInstance();
    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }


//    job.setJobContext(context);

    Method runMethod = JobUtil.resolveRunMethod(job);

    return new JavaActionJobManager(job, runMethod, context);
  }

  @Override
  public void run(Instant nominalTime, Map<String,View> views) {

    job.setNominalTime(nominalTime);
    job.setJobContext(getJobContext());

    Object[] args = JobUtil.getArgs(runMethod, views);

    try {
      runMethod.invoke(job, args);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    } catch (InvocationTargetException e) {
      throw new AppException(e);
    }

    signalOutputViews(views);
  }

  @Override
  public void writeOozieActionBlock(XMLWriter writer, Schedule schedule) {
    writer.startElement("java");
    element(writer, "job-tracker", "${jobTracker}");
    element(writer, "name-node", "${nameNode}");

    // TODO: the job-xml should probably be job-specific configuration.
    // element(writer, "job-xml", "${appConfigPath}");


    Map<String, String> settings = OozieScheduling.getJobSettings(schedule, context.getHadoopConf());

    // Write the job configuration settings.
    writer.startElement("configuration");

    // Use the hive sharelib since actions frequently interact
    // with Hive.
    property(writer, "oozie.action.sharelib.for.java", "hive2");

    for (Map.Entry<String,String> setting: settings.entrySet()) {

      property(writer, setting.getKey(), setting.getValue());
    }

    writer.endElement(); // configuration

    element(writer, "main-class", OozieScheduledJobMain.class.getCanonicalName());
    element(writer, "arg", schedule.getJobClass().getName());

    writer.endElement(); // java
  }

}
