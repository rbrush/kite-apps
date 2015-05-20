package org.kitesdk.apps.spi.oozie;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * URI hack for development purposes. Remove when using a handler for
 * Oozie Dataset URIs.
 */
public class URIShim {

  static final Pattern HDFS_PATTERN = Pattern.compile("hdfs:///user/hive/warehouse/([A-Za-z0-9_]+)\\.db/([A-Za-z0-9_]+)/?(.*)");

  static final Pattern DS_PATTERN = Pattern.compile("(?:dataset|view):hive:([A-Za-z0-9_]+)/([A-Za-z0-9_]+)\\??(.*)");

  public static String kiteToHDFS(String uri) {

    Matcher matcher = DS_PATTERN.matcher(uri);

    if (!matcher.matches()) {
      return uri;
    }

    String namespace = matcher.group(1);
    String name = matcher.group(2);
    String params = matcher.group(3);

    StringBuilder builder = new StringBuilder();

    builder.append("hdfs:///user/hive/warehouse/")
        .append(namespace)
        .append(".db/")
        .append(name);

    String[] splits = params.split("&");

    if (splits.length > 0) {

      builder.append("/")
          .append(splits[0]);

      for (int i = 1; i < splits.length; ++i) {
        builder.append("/")
            .append(splits[i]);
      }
    }

    return builder.toString();
  }

  public static String HDFSToKite(String uri) {

    Matcher matcher = HDFS_PATTERN.matcher(uri);

    if (!matcher.matches()) {
      return uri;
    }

    String namespace = matcher.group(1);
    String name = matcher.group(2);
    String params = matcher.group(3);

    StringBuilder builder = new StringBuilder();

    builder.append("view:hive:")
        .append(namespace)
        .append("/")
        .append(name);

    String[] splits = params.split("/");

    if (splits.length > 0) {

      builder.append("?")
          .append(splits[0]);

      for (int i = 1; i < splits.length; ++i) {
        builder.append("&")
            .append(splits[i]);
      }
    }

    return builder.toString();
  }
}
