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
package org.kitesdk.apps.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.IParameterSplitter;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.kitesdk.apps.AppContext;
import org.kitesdk.apps.AppException;
import org.kitesdk.apps.spi.AppDeployer;
import org.kitesdk.apps.spi.PropertyFiles;
import org.kitesdk.cli.commands.BaseCommand;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import com.google.common.io.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Parameters(commandDescription="Installs a Kite application.")
public class InstallCommand extends BaseCommand {

  public static class SingletonSplitter implements IParameterSplitter {

    @Override
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }

  @Parameter(description = "<app jar path> <class name> <destination>")
  List<String> args;

  @Parameter(description = "Configuration properties file.", names={"--properties-file"})
  String propertiesFileName;

  @Parameter(description = "A configuration setting to be used by the application. " +
      "In the form of --conf key=value. " +
      "May be specified multiple times.",
      names={"--conf"},
      splitter = SingletonSplitter.class) // Do not split a single --conf setting into multiple values.
  List<String> settings;

  private final Logger console;

  public InstallCommand(Logger console) {
    this.console = console;
  }

  /**
   * Returns a complete properties file with contents to be used for the
   * application properties, or null if no properties are set.
   */
  private File completePropertiesFile() throws IOException {
    File settingsFile = propertiesFileName != null ?
        new File(propertiesFileName) :
        null;

    // No explicit settings, so just use the original file.
    if (settings == null || settings.size() == 0) {

      return settingsFile;
    }

    // We append the specified settings to the properties file
    // so any comments or formatting are preserved.
    File appendedFile = File.createTempFile("kite", "props");

    appendedFile.deleteOnExit();

    FileOutputStream stream = new FileOutputStream(appendedFile);
    OutputStreamWriter streamWriter = new OutputStreamWriter(stream, Charsets.UTF_8);
    BufferedWriter writer = new BufferedWriter(streamWriter);

    try {
      if (settingsFile != null) {

        Files.copy(settingsFile, stream);
        writer.newLine();
      }

      for (String setting: settings) {

        // Validate the setting.
        String[] parts = setting.split("=");

        if (parts.length != 2)
          throw new IllegalArgumentException("Malformed input setting: " + setting);

        writer.append(setting);
        writer.newLine();
      }

    } finally {
      Closeables.closeQuietly(writer);
    }

    return appendedFile;
  }

  @Override
  public int run() throws IOException {

    Preconditions.checkArgument(this.args != null && this.args.size() == 3,
        "Application jar, class name, and destination must be specified");

    File appJarFile = new File(args.get(0));
    String appClassName = args.get(1);
    Path destination = new Path(args.get(2));

    console.info("Installing {} to {}.", appClassName, destination);

    FileSystem fs = FileSystem.get(getConf());

    if (fs.exists(destination)) {

      console.error("Cannot install to {} because that destination already exists.", destination);
      return 1;
    }

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

    File settingsFile = completePropertiesFile();

    if (console.isDebugEnabled()) {

      console.debug("Using the given settings:");

      List<String> lines = Files.readLines(settingsFile, Charsets.UTF_8);

      for (String line: lines) {

        console.debug(line);
      }
    }

    Map<String,String> settings = settingsFile != null ?
        PropertyFiles.load(settingsFile) :
        Collections.<String,String>emptyMap();

    AppContext context = new AppContext(settings, getConf());

    AppDeployer deployer = new AppDeployer(fs, context);

    // Load the needed libraries and the application jar
    // so they are deployed to the coordinator.
    List<File> libraryJars = getLibraryJars();
    List<File> allJars = Lists.newArrayList(libraryJars);

    allJars.add(appJarFile);

    deployer.deploy(appClass, destination, settingsFile, allJars);

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

    if (jarFiles == null)
      throw new AppException("Unable to list jar files in folder: " + file);

    return Arrays.asList(jarFiles);
  }

}
