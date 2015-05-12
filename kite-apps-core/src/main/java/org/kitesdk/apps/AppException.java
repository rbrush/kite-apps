package org.kitesdk.apps;

/**
 * Exception for application related failures.
 */
public class AppException extends RuntimeException {

  public AppException() {
    super();
  }

  public AppException(String message) {
    super(message);
  }

  public AppException(String message, Throwable t) {
    super(message, t);
  }

  public AppException(Throwable t) {
    super(t);
  }

  protected static String format(String message, Object... args) {
    String[] argStrings = new String[args.length];
    for (int i = 0; i < args.length; i += 1) {
      argStrings[i] = String.valueOf(args[i]);
    }
    return String.format(String.valueOf(message), (Object[]) argStrings);
  }
}
