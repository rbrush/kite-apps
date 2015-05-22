package org.kitesdk.apps.scheduled;

import org.junit.Test;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.test.apps.BadNameJob;
import org.kitesdk.apps.test.apps.ScheduledInputOutputApp;
import org.kitesdk.apps.test.apps.ScheduledInputOutputJob;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ScheduleTest {

  @Test
  public void testBuildSchedule() {

    Schedule schedule = new Schedule.Builder()
        .jobClass(ScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .withView("source.users", ScheduledInputOutputApp.INPUT_URI_PATTERN, 60)
        .withView("target.users", ScheduledInputOutputApp.OUTPUT_URI_PATTERN, 60)
        .build();

    assertEquals(ScheduledInputOutputJob.class,
        schedule.getJobClass());

    assertEquals("0 * * * *", schedule.getFrequency());

    assertEquals(new ScheduledInputOutputJob().getName(),
        schedule.getName());

    Map<String, Schedule.ViewTemplate> views = schedule.getViewTemplates();

    assertEquals(2, views.size());

    Schedule.ViewTemplate sourceTemplate = views.get("source.users");

    assertEquals("source.users", sourceTemplate.getName());
    assertEquals(ScheduledInputOutputApp.INPUT_URI_PATTERN, sourceTemplate.getUriTemplate());

    Schedule.ViewTemplate targetTemplate = views.get("target.users");

    assertEquals("target.users", targetTemplate.getName());
    assertEquals(ScheduledInputOutputApp.OUTPUT_URI_PATTERN, targetTemplate.getUriTemplate());
  }

  @Test(expected = AppException.class)
  public void testBadJobName() {

    new Schedule.Builder().jobClass(BadNameJob.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoSuchName() {

    new Schedule.Builder()
        .jobClass(ScheduledInputOutputJob.class)
        .withView("bogus.name", ScheduledInputOutputApp.INPUT_URI_PATTERN, 60)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testViewNotProvided() {
    new Schedule.Builder()
        .jobClass(ScheduledInputOutputJob.class)
        .build();
  }
}
