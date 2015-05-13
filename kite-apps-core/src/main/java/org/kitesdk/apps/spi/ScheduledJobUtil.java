package org.kitesdk.apps.spi;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.apps.scheduled.ScheduledJob;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Helper class for working with scheduled jobs.
 */
public class ScheduledJobUtil {

  public static Map<String, DataIn> getInputs(Class<? extends ScheduledJob> jobClass) {

    Method runMethod = resolveRunMethod(jobClass);

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();

    Map<String,DataIn> inputs = Maps.newHashMap();

    for (Annotation[] annotations: paramAnnotations) {
      for (Annotation annotation: annotations) {
        if (DataIn.class.equals(annotation.annotationType()))
          inputs.put(((DataIn) annotation).name(), (DataIn) annotation);
      }
    }

    return inputs;
  }

  public static Map<String,DataOut> getOutputs(Class<? extends ScheduledJob> jobClass) {

    Method runMethod = resolveRunMethod(jobClass);

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();

    Map<String,DataOut> outputs = Maps.newHashMap();

    for (Annotation[] annotations: paramAnnotations) {
      for (Annotation annotation: annotations) {
        if (DataOut.class.equals(annotation.annotationType()))
          outputs.put(((DataOut) annotation).name(), (DataOut) annotation);
      }
    }

    return outputs;
  }

  public static Method resolveRunMethod(Class<? extends ScheduledJob> jobClass) {

    Method runMethod = null;

    for (Method method: jobClass.getMethods()) {

      if ("run".equals(method.getName())) {

        if (runMethod != null)
          throw new AppException("Multiple run methods found on scheduled job "
              + jobClass.getName());

        runMethod = method;
      }
    }

    if (runMethod == null)
      throw new AppException("Could not find run method on class "  +
          jobClass.getName());

    return runMethod;
  }
}
