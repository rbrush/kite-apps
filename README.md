## Kite Apps
Kite Apps is a prescriptive approach for writing, deploying and managing applications based on Hadoop and Kite. Developers use a simple fluent-style API to schedule jobs and define Kite views as inputs and outputs. This library handles all scheduling work for the developer, generating Oozie coordinators and workflows to satisfy the described schedule.

## Writing a Kite Application
Users of this library work with two major concepts:

* A ScheduledJob, which is a unit of work executed based on a cron-like schedule and availability of its inputs.
* An Application, which installs resources (like Kite datasets) and ScheduledJobs.

Here is an example of a job:

```java
public class ExampleJob extends AbstractJob {

  public void run(@DataIn(name="example.events", type=ExampleEvent.class) View<ExampleEvent> input,
                  @DataOut(name="example.output", type=ExampleOutput.class) View<ExampleOutput> output) {

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
        .frequency("* * * * *")
        .onReady("example.events", EVENT_URI_PATTERN)
        .withView("example.output", OUTPUT_URI_PATTERN)
        .build();

    schedule(schedule);
  }
}
```

The schedule of a job provides a cron-style frequency and a pattern to create the URI for each time a job instance is launched. Each job run is done at a _nominal time_, based on the Oozie concept of the same name. That nominal time is used to populate the given URI pattern to identify the input and output datasets used in a job. In the above example, a job run at nominal time 2015-05-12T18:00Z would be wired to the input of ```view:hive:example/events?year=2015&month=05&day=12&hour=18``` and the output of ```view:hive:example/output?year=2015&month=05&day=12&hour=18```. Jobs may have as many input and outputs as the user needs.

See the kite-apps-examples projects for several complete examples.

## Installing the CLI
A CLI pre-packaged for CDH5 is available in the kite-apps-tools-cdh5 project. This project produces a gzipped TAR file that contains a kite-apps shell script to launch the CLI. Example usage is below.

## Setup
The examples assume that Hadoop, Hive, and Oozie configuration settings are visible. These are often set in Hadoop environments. Here is an example setting them up on a Cloudera Quickstart VM:

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf
export OOZIE_URL=http://quickstart.cloudera:11000/oozie
```

With these in place, simply untar the Kite Apps tool and it will be ready for use:

```bash
tar xzf kite-apps-tools-cdh5-0.1.0-SNAPSHOT.tar.gz
cd kite-apps-tools-cdh5-0.1.0-SNAPSHOT
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
* Unit tests and documentation are very lacking
* Use a Dataset URI Handler for Oozie integration rather than the current (fragile) shim.
* Add a way for job configuration to be externally defined, possibly providing a job-settings.xml for each Kite job.
* Add support for a StreamingJob, based on Spark Streaming and Kafka, to complement the existing ScheduledJob.
* Move Kite Apps and underlying libraries to a shared library to avoid duplicating them in every application

