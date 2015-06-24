## Kite Apps
Kite Apps is a prescriptive approach for writing, deploying and managing applications on Hadoop and Kite. Developers use a simple fluent Java API to schedule Crunch or Spark jobs, wiring them to Kite views as inputs and outputs. This library handles all scheduling work, generating and deploying the needed Oozie coordinators, workflows, and application libraries itself.

This library is still maturing and may be subject to non-passive changes. It has been tested on CDH 5.4.

## Writing a Kite Application
Users of this library work with two major concepts:

* A ScheduledJob, which is a unit of work executed based on a cron-like schedule and availability of its inputs.
* An Application, which installs resources (like Kite datasets) and ScheduledJobs.

Here is an example of a job:

```java
public class ExampleJob extends AbstractJob {

  public void run(@DataIn(name="example.events", type=ExampleEvent.class)
                  View<ExampleEvent> input,
                  @DataOut(name="example.output", type=ExampleOutput.class)
                  View<ExampleOutput> output) {

     // Do work here.
  }
}
```

Notice the ```DataIn``` and ```DataOut``` annotations on the run method. These identify Kite views that are provided to the job based on the inputs and outputs used for the run. These are defined when the job is scheduled by the application, like this:

```java
public class ExampleApp extends AbstractApplication {

  static final String EVENT_URI_PATTERN = "view:hive:example/events" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}";

  static final String OUTPUT_URI_PATTERN = "view:hive:example/output" +
      "?year=${YEAR}&month=${MONTH}&day=${DAY}&hour=${HOUR}";

  public void setup(Configuration conf) {

    // Schedule our report to run every five minutes.
    Schedule schedule = new Schedule.Builder()
        .jobClass(ExampleJob.class)
        .frequency("0 * * * *")
        .withInput("example.events", EVENT_URI_PATTERN, "0 * * * *")
        .withOutput("example.output", OUTPUT_URI_PATTERN)
        .build();

    schedule(schedule);
  }
}
```

The schedule of a job provides a cron-style frequency and a pattern to create the URI for each time a job instance is launched. Each job run is done at a _nominal time_, based on the Oozie concept of the same name. That nominal time is used to populate the given URI pattern to identify the input and output datasets used in a job. In the above example, a job run at nominal time 2015-05-12T18:00Z would be wired to the input of ```view:hive:example/events?year=2015&month=05&day=12&hour=18``` and the output of ```view:hive:example/output?year=2015&month=05&day=12&hour=18```. Jobs may have as many input and outputs as the user needs.

See the kite-apps-examples projects for several complete examples.

## Installation
This library has two parts to install: Oozie URI support for Kite datasets and a command-line tool for deploying applications.

### Installing the Oozie URI Handler
The Oozie URI handler available starting with Kite 1.1 must be installed by hand for the time being. Better documentation to include with Oozie is being tracked in [OOZIE-2269](https://issues.apache.org/jira/browse/OOZIE-2269). For now, users can take these steps:

First, the following files should be installed where Oozie can see them. Installing into ```/var/lib/oozie``` works on a Cloudera quickstart machine. The jars are:

* kite-data-core-1.1.0.jar
* kite-data-oozie-1.1.0.jar
* kite-hadoop-compatibility-1.1.0.jar
* kite-data-hive-1.1.0.jar
* commons-jexl-2.1.1.jar
* jackson-core-2.3.1.jar
* jackson-databind-2.3.1.jar

These libraries can be found in Maven repositories or in the lib folder of the kite-apps-tools assembly.

Second, the following must be added to the oozie-site.xml. (Cloudera users can do so via the oozie-site safety valve in Cloudera manager.)

```xml
<property>
  <name>oozie.service.URIHandlerService.uri.handlers</name>
  <value>org.apache.oozie.dependency.FSURIHandler,org.apache.oozie.dependency.HCatURIHandler,org.kitesdk.data.oozie.KiteURIHandler</value>
</property>
```

Finally, Oozie needs to be restarted for these changes to take effect.

### Installing the CLI
A CLI pre-packaged is available in the kite-apps-tools project. This project produces a gzipped TAR file that contains a kite-apps shell script to launch the CLI. Example usage is below.

## Setup
The examples assume that Hadoop, Hive, and Oozie configuration settings are visible. These are often set in Hadoop environments. On a Cloudera quickstart VM, one simply needs to define the OOZIE_URL:

```bash
export OOZIE_URL=http://quickstart.cloudera:11000/oozie
```

With this in place, simply untar the Kite Apps tool and it will be ready for use:

```bash
tar xzf kite-apps-tools-0.1.0-SNAPSHOT.tar.gz
cd kite-apps-tools-0.1.0-SNAPSHOT
```

## Running the examples
The kite-apps-examples sub-project offers several examples of using kite-apps. These include data generation and triggering Crunch jobs based on the presence of input data. To install an application that generates test data every minute, run the following:

```bash
bin/kite-apps install /path/to/example/kite-apps-examples-0.1.0-SNAPSHOT.jar \
org.kitesdk.apps.examples.generate.DataGeneratorApp \
/path/to/install/dir/on/hdfs
```

This will create a Kite dataset backed by Hive and write ten test records to it every minute. Another example application will consume this test data, triggered when a new view is written. This can be installed with:

```bash
bin/kite-apps install /path/to/example/target/kite-apps-examples-0.1.0-SNAPSHOT.jar \
org.kitesdk.apps.examples.triggered.TriggeredApp \
/path/to/install/dir/on/hdfs
```

## Kite Apps layout
Kite Applications are installed to a target directory, which contains the following structure:

```
<app-root>/lib -- The set of JARs
<app-root>/oozie -- All Oozie artifacts for the application are in here.
<app-root>/oozie/bundle.xml -- the Oozie bundle for the entire application
<app-root>/oozie/workflows -- Each ScheduledJob has a workflow directory that is
                              the fully-qualified class name of the job.
<app-root>/oozie/coordinators -- Each ScheduledJob has a coordinator directory that is
                                 the fully-qualified class name of the job.
```

This will be expanded in the future, with separate directories to contain user-editable configuration or other content.

## Outstanding items
* Improved documentation and unit tests.
* Add a way for job configuration to be externally defined, possibly providing a job-settings.xml for each Kite job. See [issue 3](https://github.com/rbrush/kite-apps/issues/3).
* Add support for a StreamingJob, based on Spark Streaming and Kafka, to complement the existing ScheduledJob. See [issue 4](https://github.com/rbrush/kite-apps/issues/4).
* Tooling to simplify upgrades and uninstallation of applications. See [issue 10](https://github.com/rbrush/kite-apps/issues/10).
