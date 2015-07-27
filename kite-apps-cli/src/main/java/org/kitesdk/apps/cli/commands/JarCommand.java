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
import org.kitesdk.cli.commands.BaseCommand;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Command to run an arbitrary JAR file with all Kite apps dependencies
 * on the classpath.
 */
@Parameters(commandDescription="Runs an arbitrary JAR.")
public class JarCommand extends BaseCommand {

  @Parameter(description = "<app jar path> <class name> <app-args>")
  List<String> args;


  private final Logger console;

  public JarCommand(Logger console) {
    this.console = console;
  }

  @Override
  public int run() throws IOException {

    String appJar = args.get(0);
    String className = args.get(1);
    List<String> appArgs = args.subList(2, args.size());

    File appJarFile = new File(appJar);

    if (!appJarFile.exists() || appJarFile.isDirectory()) {

      console.error("File {} is not a valid JAR file.", appJar);
      return 1;
    }

    URL appJarURL = appJarFile.toURI().toURL();

    URLClassLoader loader = new URLClassLoader(new URL[] {appJarURL},
        Thread.currentThread().getContextClassLoader());

    try {
      Class target = loader.loadClass(className);

      Method mainMethod = target.getMethod("main", new Class[]{String[].class});

      Object[] args = new Object[1];

      args[0] = appArgs.toArray(new String[appArgs.size()]);

      mainMethod.invoke(null, args);

    } catch (ClassNotFoundException e) {
      console.error("Unable to load class", e);
      return 1;
    } catch (NoSuchMethodException e) {
      console.error("Unable to resolve main method", e);
      return 1;
    } catch (InvocationTargetException e) {
      console.error("Exception thrown in main method", e);
      return 1;
    } catch (IllegalAccessException e) {
      console.error("Unable to run main method", e);
      return 1;
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Collections.emptyList();
  }
}
