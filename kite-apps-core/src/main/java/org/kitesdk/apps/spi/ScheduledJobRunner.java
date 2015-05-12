package org.kitesdk.apps.spi;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.apps.scheduled.ScheduledJob;
import org.kitesdk.data.View;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

/**
 * Runs a scheduled job with the given nominal time.
 */
public class ScheduledJobRunner {

  private final ScheduledJob job;

  private final Configuration conf;

  private final Method runMethod;

  private ScheduledJobRunner(ScheduledJob job,
                             Method runMethod, Configuration conf) {
    this.job = job;
    this.runMethod = runMethod;
    this.conf = conf;
  }

  public static ScheduledJobRunner create(Class<? extends ScheduledJob> jobClass,
                                          Configuration conf) {

    ScheduledJob job;

    try {
      job = jobClass.newInstance();
    } catch (InstantiationException e) {
      throw new AppException(e);
    } catch (IllegalAccessException e) {
      throw new AppException(e);
    }

    job.setConf(conf);

    Method runMethod = ScheduledJobUtil.resolveRunMethod(jobClass);


    return new ScheduledJobRunner(job, runMethod, conf);

  }

  private Object[] getArgs(Map<String,View> views) {

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();
    Object[] args = new Object[paramAnnotations.length];

    for (int i = 0; i < paramAnnotations.length; ++i) {

      Annotation[] annotations = paramAnnotations[i];

      for (Annotation annotation: annotations) {
        if (DataOut.class.equals(annotation.annotationType())) {

          View view = views.get(((DataOut) annotation).name());

          if (view == null) {
            throw new AppException("No view defined for job parameter: ");
          }

          args[i] = view;
        }

        if (DataIn.class.equals(annotation.annotationType())) {

          View view = views.get(((DataIn) annotation).name());

          if (view == null) {
            throw new AppException("No view defined for job parameter: ");
          }

          args[i] = view;
        }
      }
    }

    return args;
  }


  public void run(Instant nominalTime) {
    run(nominalTime, Collections.<String,View>emptyMap());
  }

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
  }
}
