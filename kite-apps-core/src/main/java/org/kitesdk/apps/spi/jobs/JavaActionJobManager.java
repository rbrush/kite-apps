package org.kitesdk.apps.spi.jobs;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.plexus.util.xml.XMLWriter;
import org.codehaus.plexus.util.xml.XmlStreamWriter;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
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


  public JavaActionJobManager(SchedulableJob job, Method runMethod, Configuration conf) {
    super(job, runMethod, conf);
  }

  public static JavaActionJobManager create(Class<? extends SchedulableJob> jobClass,
                                            Configuration conf) {

    SchedulableJob job;

    try {
      job = jobClass.newInstance();
    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }

    job.setConf(conf);

    Method runMethod = resolveRunMethod(job);

    return new JavaActionJobManager(job, runMethod, conf);
  }

  @Override
  public void run(Instant nominalTime, Map<String,View> views) {

    job.setNominalTime(nominalTime);

    Object[] args = getArgs(views);

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


    Map<String, String> settings = OozieScheduling.getJobSettings(schedule, getConf());

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
