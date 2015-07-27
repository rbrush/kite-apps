/**
 * Copyright 2015 Cerner Corporation.
 * Copyright 2015 Cloudera, Inc.
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
package org.kitesdk.apps.cli;

import com.beust.jcommander.JCommander;

import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.kitesdk.apps.cli.commands.InstallCommand;
import org.kitesdk.apps.cli.commands.JarCommand;
import org.kitesdk.cli.Command;
import org.kitesdk.cli.Help;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * This class is based on logic from kite-tools project. Ultimately
 * this logic should be factored out to eliminate this duplication.
 */
public class Main extends Configured implements Tool {

  public static void main (String[] args) throws Exception {
    // reconfigure logging with the kite CLI configuration
    PropertyConfigurator.configure(
        Main.class.getResource("/kite-apps-cli-logging.properties"));
    Logger console = LoggerFactory.getLogger(Main.class);

    Configuration conf = new Configuration();

    conf.addResource("yarn-site.xml");
    conf.addResource("hive-site.xml");

    int rc = ToolRunner.run(conf, new Main(console), args);
    System.exit(rc);
  }

  @Parameter(names = {"-v", "--verbose", "--debug"},
      description = "Print extra debugging information")
  private boolean debug = false;

  @VisibleForTesting
  @Parameter(names="--dollar-zero",
      description="A way for the runtime path to be passed in", hidden=true)
  String programName = DEFAULT_PROGRAM_NAME;

  @VisibleForTesting
  static final String DEFAULT_PROGRAM_NAME = "kite-apps";

  private static Set<String> HELP_ARGS = ImmutableSet.of("-h", "-help", "--help", "help");


  private final Logger console;
  private final JCommander jc;

  private Main(Logger console) {
    this.console = console;
    this.jc = new JCommander(this);
    jc.addCommand("install", new InstallCommand(console));
    jc.addCommand("jar", new JarCommand(console));

  }

  @Override
  public int run(String[] args) throws Exception {

    if (getConf() != null) {
      DefaultConfiguration.set(getConf());
    }

    try {
      jc.parse(args);
    } catch (MissingCommandException e) {
      console.error(e.getMessage());
      return 1;
    } catch (ParameterException e) {
      console.error(e.getMessage());
      return 1;
    }

    // configure log4j
    if (debug) {
      org.apache.log4j.Logger console = org.apache.log4j.Logger.getLogger(Main.class);
      console.setLevel(Level.DEBUG);
    }

    String parsed = jc.getParsedCommand();
    if (parsed == null) {

      console.error("Unable to parse command.");
      return 1;
    }

    Command command = (Command) jc.getCommands().get(parsed).getObjects().get(0);
    if (command == null) {

      console.error("Unable to find command.");
      return 1;
    }

    try {
      if (command instanceof Configurable) {
        ((Configurable) command).setConf(getConf());
      }
      return command.run();
    } catch (IllegalArgumentException e) {
      if (debug) {
        console.error("Argument error", e);
      } else {
        console.error("Argument error: {}", e.getMessage());
      }
      return 1;
    } catch (IllegalStateException e) {
      if (debug) {
        console.error("State error", e);
      } else {
        console.error("State error: {}", e.getMessage());
      }
      return 1;
    } catch (ValidationException e) {
      if (debug) {
        console.error("Validation error", e);
      } else {
        console.error("Validation error: {}", e.getMessage());
      }
      return 1;
    } catch (DatasetNotFoundException e) {
      if (debug) {
        console.error("Cannot find dataset", e);
      } else {
        // the error message already contains "No such dataset: <name>"
        console.error(e.getMessage());
      }
      return 1;
    } catch (DatasetIOException e) {
      if (debug) {
        console.error("IO error", e);
      } else {
        console.error("IO error: {}", e.getMessage());
      }
      return 1;
    } catch (Exception e) {
      if (debug) {
        console.error("Unknown error", e);
      } else {
        console.error("Unknown error: {}", e.getMessage());
      }
      return 1;
    }
  }
}
