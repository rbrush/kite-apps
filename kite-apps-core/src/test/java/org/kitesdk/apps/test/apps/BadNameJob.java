package org.kitesdk.apps.test.apps;

import org.kitesdk.apps.scheduled.AbstractSchedulableJob;

/**
 * Test class with an invalid name.
 */
public class BadNameJob extends AbstractSchedulableJob  {

  @Override
  public String getName() {
    return "invalid.name.with.periods";
  }

  public void run() {
    // Do nothing.
  }
}
