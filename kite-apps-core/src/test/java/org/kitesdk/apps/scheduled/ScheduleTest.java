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
package org.kitesdk.apps.scheduled;

import org.joda.time.Instant;
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
        .jobName("scheduled-input-output")
        .jobClass(ScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .withInput("source_users", ScheduledInputOutputApp.INPUT_URI_PATTERN, "0 * * * *")
        .withOutput("target_users", ScheduledInputOutputApp.OUTPUT_URI_PATTERN)
        .build();

    assertEquals(ScheduledInputOutputJob.class,
        schedule.getJobClass());

    assertEquals("0 * * * *", schedule.getFrequency());

    assertEquals(new ScheduledInputOutputJob().getName(),
        schedule.getName());

    Map<String, Schedule.ViewTemplate> views = schedule.getViewTemplates();

    assertEquals(2, views.size());

    Schedule.ViewTemplate sourceTemplate = views.get("source_users");

    assertEquals("source_users", sourceTemplate.getName());
    assertEquals(ScheduledInputOutputApp.INPUT_URI_PATTERN, sourceTemplate.getUriTemplate());

    Schedule.ViewTemplate targetTemplate = views.get("target_users");

    assertEquals("target_users", targetTemplate.getName());
    assertEquals(ScheduledInputOutputApp.OUTPUT_URI_PATTERN, targetTemplate.getUriTemplate());
  }

  @Test
  public void testStartNextHour() {

    Instant startTime = Instant.parse("2015-06-10T02:42:37.52Z");
    Instant effectiveStart = Instant.parse("2015-06-10T03:00:00.00Z");

    Schedule schedule = new Schedule.Builder()
        .jobName("scheduled-input-output")
        .jobClass(ScheduledInputOutputJob.class)
        .frequency("0 * * * *")
        .startAt(startTime)
        .withInput("source_users", ScheduledInputOutputApp.INPUT_URI_PATTERN, "0 * * * *")
        .withOutput("target_users", ScheduledInputOutputApp.OUTPUT_URI_PATTERN)
        .build();

    assertEquals(effectiveStart, schedule.getStartTime());
  }


  @Test(expected = AppException.class)
  public void testBadJobName() {

    new Schedule.Builder()
        .jobName("invalid.name.with.periods")
        .jobClass(BadNameJob.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoSuchName() {

    new Schedule.Builder()
        .jobName("scheduled-input-output")
        .jobClass(ScheduledInputOutputJob.class)
        .withView("bogus.name", ScheduledInputOutputApp.INPUT_URI_PATTERN, 60)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testViewNotProvided() {
    new Schedule.Builder()
        .jobName("scheduled-input-output")
        .jobClass(ScheduledInputOutputJob.class)
        .build();
  }
}
