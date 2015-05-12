package org.kitesdk.apps.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.kitesdk.apps.spi.AppDeployer;
import org.kitesdk.cli.commands.BaseCommand;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Parameters(commandDescription="Installs a Kite application.")
public class InstallCommand extends BaseCommand {

  @Parameter(description = "<app jar path> <class name> <destination>")
  List<String> args;

  private final Logger console;

  public InstallCommand(Logger console) {
    this.console = console;
  }

  @Override
  public int run() throws IOException {

    Preconditions.checkArgument(this.args != null && this.args.size() == 3,
        "Application jar, class name, and destination must be specified");

    File appJarFile = new File(args.get(0));
    String appClassName = args.get(1);
    Path destination = new Path(args.get(2));

    if (!appJarFile.exists() || appJarFile.isDirectory()) {
      console.error("File {} is not a valid JAR file.", appJarFile.getAbsolutePath());
      return 1;
    }

    ClassLoader appClassLoader = getAppClassloader(appJarFile);

    Thread.currentThread().setContextClassLoader(appClassLoader);

    Class appClass;

    try {
      appClass = appClassLoader.loadClass(appClassName);
    } catch (ClassNotFoundException e) {
      console.error("Unabled to load application class {}", appClassName);
      console.error("Load error.", e);
      return 1;
    }

    FileSystem fs = FileSystem.get(getConf());

    AppDeployer deployer = new AppDeployer(fs, getConf());

    // Load the needed libraries and the application jar
    // so they are deployed to the coordinator.
    // TODO: Should we move the Kite and its dependencies to a shared library?
    List<File> libraryJars = getLibraryJars();
    List<File> allJars = Lists.newArrayList(libraryJars);

    allJars.add(appJarFile);

    deployer.deploy(appClass, destination, allJars);

    console.info("Application JAR installed.");

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Collections.emptyList();
  }

  /**
   * Returns a classloader that has visibility to the application artifacts.
   */
  private ClassLoader getAppClassloader(File appJar) {

    URL[] appJarURLs;

    try {
      appJarURLs = new URL[] {appJar.toURI().toURL()};
    } catch (MalformedURLException e) {

      // This should not be possible.
      throw new AssertionError(e);
    }

    URLClassLoader loader = new URLClassLoader(appJarURLs,
        Thread.currentThread().getContextClassLoader());

    return loader;
  }

  private static final List<File> getLibraryJars() {

    // Current implementation assumes that library files
    // are in the same directory, so locate it and
    // include it in the project library.

    // This is ugly, using the jobConf logic to identify the containing
    // JAR. There should be a better way to do this.
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(InstallCommand.class);
    String containingJar = jobConf.getJar();

    File file = new File(containingJar).getParentFile();

    File[] jarFiles = file.listFiles();

    return Arrays.asList(jarFiles);
  }

}
