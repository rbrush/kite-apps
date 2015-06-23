package org.kitesdk.apps.spi.oozie;

import org.joda.time.Instant;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.kitesdk.apps.spi.oozie.CronConverter.toFrequency;
import static org.kitesdk.apps.spi.oozie.CronConverter.nextInstant;

public class CronConverterTest {

  /**
   * Validate that a given schedule and current time creates an Oozie
   * frequency with the expected value and start time.
   */
  private void testInterval(String schedule, String expectedFrequency,
                            String currentTime, String expectedStartTime) {

    assertEquals(expectedFrequency, toFrequency(schedule));

    assertEquals(Instant.parse(expectedStartTime),
        nextInstant(schedule, Instant.parse(currentTime)));
  }

  @Test
  public void everyMinute() {

    testInterval("* * * * *",
        "1",
        "2015-06-10T23:42:37.00Z",
        "2015-06-10T23:43:00.00Z");
  }

  @Test
  public void everyFiveMinutes() {

    // the start time should be line up with an hour.
    testInterval("*/5 * * * *",
        "5",
        "2015-06-10T23:42:37.00Z",
        "2015-06-10T23:45:00.00Z");

    // ensure we handle rolling into the next day
    testInterval("*/5 * * * *",
        "5",
        "2015-06-10T23:56:37.00Z",
        "2015-06-11T00:00:00.00Z");

    testInterval("*/5 * * * *",
        "5",
        "2015-06-10T07:58:37.00Z",
        "2015-06-10T08:00:00.00Z");
  }

  @Test
  public void hourly() {

    testInterval("0 * * * *",
        "${coord:hours(1)}",
        "2015-06-10T02:42:37.52Z",
        "2015-06-10T03:00:00.00Z");

    // test start at a five minute offset from the hour.
    testInterval("5 * * * *",
        "${coord:hours(1)}",
        "2015-06-10T02:42:37.52Z",
        "2015-06-10T03:05:00.00Z");

    // test roll ot next day.
    testInterval("0 * * * *",
        "${coord:hours(1)}",
        "2015-06-10T23:42:37.00Z",
        "2015-06-11T00:00:00.00Z");
  }

  @Test
  public void daily() {

    // Runs daily at 1:00.
    testInterval("0 1 * * *",
        "${coord:days(1)}",
        "2015-06-10T02:42:37.52Z",
        "2015-06-11T01:00:00.00Z");

    // Runs daily at 9:30.
    testInterval("30 9 * * *",
        "${coord:days(1)}",
        "2015-06-10T02:42:37.52Z",
        "2015-06-10T09:30:00.00Z");

  }

  @Test
  public void weekly() {

    // Runs every seven days starting at noon.
    testInterval("0 12 */7 * *",
        "${coord:days(7)}",
        "2015-06-10T02:42:37.52Z",
        "2015-06-10T12:00:00.00Z");

    // Runs every seven days starting at noon.
    testInterval("0 12 */7 * *",
        "${coord:days(7)}",
        "2015-06-10T23:42:37.52Z",
        "2015-06-11T12:00:00.00Z");
  }
}
