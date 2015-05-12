package org.kitesdk.apps.spi;

import com.google.common.collect.Lists;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.scheduled.DataIn;
import org.kitesdk.apps.scheduled.DataOut;
import org.kitesdk.apps.scheduled.ScheduledJob;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Helper class for working with scheduled jobs.
 */
public class ScheduledJobUtil {

  public static List<DataIn> getInputs(Class<? extends ScheduledJob> jobClass) {

    Method runMethod = resolveRunMethod(jobClass);

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();

    List<DataIn> inputs = Lists.newArrayList();

    for (Annotation[] annotations: paramAnnotations) {
      for (Annotation annotation: annotations) {
        if (DataIn.class.equals(annotation.annotationType()))
          inputs.add((DataIn) annotation);
      }
    }

    return inputs;
  }

  public static List<DataOut> getOutputs(Class<? extends ScheduledJob> jobClass) {

    Method runMethod = resolveRunMethod(jobClass);

    Annotation[][] paramAnnotations = runMethod.getParameterAnnotations();

    List<DataOut> outputs = Lists.newArrayList();

    for (Annotation[] annotations: paramAnnotations) {
      for (Annotation annotation: annotations) {
        if (DataOut.class.equals(annotation.annotationType()))
          outputs.add((DataOut) annotation);
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
