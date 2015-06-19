package org.kitesdk.apps.spi.oozie;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

import java.util.regex.Pattern;

/**
 * Utility class for converting cron-based definitions to Oozie frequencies.
 *
 * This may be replaced in the future should Oozie support cron definitions
 * in frequencies. See https://issues.apache.org/jira/browse/OOZIE-1431.
 */
public class CronConverter {

  private static final Pattern NUMBER = Pattern.compile("[0-9]+");

  private static final Pattern INTERVAL = Pattern.compile("\\*/[0-9]+");

  private static void validate(String cronSchedule) {

    String[] parts = cronSchedule.split(" ");

    if (parts.length != 5)
      throw new IllegalArgumentException("Invalid cron schedule:" + cronSchedule);

    // Minute, hour, day must all be valid
    if (!isValid(parts[0]) ||
        !isValid(parts[1]) ||
        !isValid(parts[2]))
      throw new IllegalArgumentException("Invalid cron schedule: " + cronSchedule);

    // Only wildcards are currently supported in month and day-of-week parts.
    if (!isWildCard(parts[3]) || !isWildCard(parts[4]))
      throw new IllegalArgumentException("Month or day-of-week cron schedules " +
          "not currently supported. Schedule: " + cronSchedule);

  }

  private static boolean isWildCard(String cronPart) {
    return cronPart.equals("*");
  }

  private static boolean isConstant(String cronPart) {
    return NUMBER.matcher(cronPart).matches();
  }

  private static boolean isInterval(String cronPart) {
    return INTERVAL.matcher(cronPart).matches();
  }

  private static boolean isValid(String cronPart) {
    return isConstant(cronPart) ||
        isInterval(cronPart) ||
        isWildCard(cronPart);
  }

  /**
   * Returns the interval value. The given cron part
   * must be a valid interval (i.e., isInterval(...) returns true)
   */
  private static int getInterval(String cronPart) {

    String intervalPart = cronPart.substring(cronPart.indexOf("/") + 1);

    return Integer.parseInt(intervalPart);
  }

  /**
   * Converts a given cron-style schedule to an Oozie frequency,
   * if supported. Throws an IllegalArgumentException
   * if a conversion isn't possible.
   */
  public static String toFrequency(String cronSchedule) {

    validate(cronSchedule);

    String[] splits = cronSchedule.split(" ");

    String minute = splits[0];
    String hour = splits[1];
    String day = splits [2];


    if (isWildCard(minute)) {
      return "1";
    }

    if (isInterval(minute)) {
      return Integer.toString(getInterval(minute));
    }

    if (isWildCard(hour)) {
      return "${coord:hours(1)}";
    }

    if (isInterval(hour)) {
      return "${coord:hours(" + getInterval(hour) + ")}";
    }

    if (isWildCard(day)) {
      return "${coord:days(1)}";
    }

    // The day must be an interval. If it wasn't, then
    // the schedule was malformed and should have yielded
    // an exception.
    assert(isInterval(day));

    return "${coord:days(" + getInterval(day) + ")}";
  }

  public static Instant nextInstant(String cronSchedule, Instant current) {

    DateTime currentTime = new DateTime(current, DateTimeZone.UTC)
        .withSecondOfMinute(0)
        .withMillisOfSecond(0);

    validate(cronSchedule);

    String[] splits = cronSchedule.split(" ");

    String minutePart = splits[0];
    String hourPart = splits[1];
    String dayPart = splits [2];


    // TODO: fold these together like the hour.
    if (isWildCard(minutePart)) {
      return currentTime.plusMinutes(1)
          .toInstant();
    }

    if (isInterval(minutePart)) {

      // Roll minutes forward until we hit a start time
      // that matches the cron interval.
      int interval = getInterval(minutePart);

      DateTime next = currentTime.withSecondOfMinute(0)
          .plusMinutes(1);

      while (!(next.getMinuteOfHour() % interval == 0)) {

        next = next.plusMinutes(1);
      }

      return next.toInstant();
    }

    assert(isConstant(minutePart));

    // The minute part must be a constant, so
    // simply get the value.
    int minute = Integer.parseInt(minutePart);

    // The schedule is based on hours.
    if (!isConstant(hourPart)) {

      int hourModulus = isWildCard(hourPart) ? 1 : getInterval(hourPart);

      DateTime next = currentTime.withMinuteOfHour(minute);

      while (next.isBefore (current) ||
          !(next.getHourOfDay() % hourModulus == 0)) {

        next = next.plusHours(1);
      }

      return next.toInstant();
    }

    int hour = Integer.parseInt(hourPart);

    // The schedule is based on days, and therfore the day cannot
    // be a constant. This is is checked in validation as well.
    assert (!isConstant(dayPart));

    DateTime next = currentTime.withMinuteOfHour(minute).withHourOfDay(hour);

    while (next.isBefore (current)) {

      next = next.plusDays(1);
    }

    return next.toInstant();
  }
}
