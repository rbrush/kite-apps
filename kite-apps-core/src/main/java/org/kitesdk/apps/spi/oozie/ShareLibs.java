package org.kitesdk.apps.spi.oozie;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Support for working with Oozie shared libraries.
 */
public class ShareLibs {

  private static final String DEFAULT_SHARE_LIB_PATH = "/user/oozie/share/lib";



  /**
   * Based on ShareLibService.getLatestLibPath, which is not available
   * in client libraries.
   */
  private static Path getLatestLibPath(FileSystem fs, Path rootDir) throws IOException {
    Date max = new Date(0L);
    Path path = null;
    PathFilter directoryFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("lib_");
      }
    };

    FileStatus[] files = fs.listStatus(rootDir, directoryFilter);
    for (FileStatus file : files) {
      String name = file.getPath().getName().toString();
      String time = name.substring("lib_".length());
      Date d = null;
      try {
        d = new SimpleDateFormat("yyyyMMddHHmmss").parse(time);
      }
      catch (ParseException e) {
        continue;
      }
      if (d.compareTo(max) > 0) {
        path = file.getPath();
        max = d;
      }
    }
    //If there are no timestamped directories, fall back to root directory
    if (path == null) {
      path = rootDir;
    }
    return path;
  }

  /**
   * Returns a list of absolute paths to the JARs in the given shared library,
   * or an empty list if the given shared library does not exist.
   */
  public static List<Path> jars(Configuration conf, String sharedLibName) throws IOException {

    List<Path> sharedLibJars = Lists.newArrayList();

    // Oozie does not provide a client-facing way to enumerate sharedlib
    // contents, so we look at the shared library location ourselves.

    FileSystem fs = FileSystem.get(conf);

    String sharelib = conf.get("system.libpath", DEFAULT_SHARE_LIB_PATH);

    Path sharelibPath = new Path(sharelib);

    if (fs.exists(sharelibPath)) {

      Path latestParentPath = getLatestLibPath(fs, sharelibPath);

      Path libPath = new Path(latestParentPath, sharedLibName);

      if (fs.exists(libPath)) {

        FileStatus[] files = fs.listStatus(libPath);

        for (FileStatus file: files) {
          sharedLibJars.add(file.getPath());
        }
      }
    }

    return sharedLibJars;
  }
}
