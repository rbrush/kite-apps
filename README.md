## Kite Apps [![Build Status](https://travis-ci.org/mkwhitacre/kite-apps.png?branch=master)]

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

## Writing a Streaming Job
Regularly scheduled jobs via Oozie are simple to operate and understand and meet many use cases. However, they are not sufficient for workloads that require very low latency. Therefore we also support stream-based processing. This is accomplished via Spark Streaming, but other stream-based models could be added in the future.

Here is an example of a very simple streaming job:


```java
public class SparkStreamingJob extends AbstractStreamingSparkJob {

  public void run(@DataIn(name = "event.stream", type = ExampleEvent.class)
                  JavaDStream<ExampleEvent> stream,
                  @DataOut(name = "event.output", type = ExampleEvent.class)
                  Dataset<ExampleEvent> output) {

    SparkDatasets.save(stream, output);
  }

  @Override
  public String getName() {
    return "test-event-stream";
  }
}
```

This job simply writes data from the given DStream to the given Kite dataset, but jobs may perform arbitrary Spark Streaming operations on the given DStream. A Kite Application can install such jobs in the ```setup``` method much like the scheduled job example above. Simply add a segment like this:

```java
StreamDescription streamDescription = new StreamDescription.Builder()
    .jobClass(SparkStreamingJob.class)
    .withStream("event.stream", KafkaUtils.kafkaProps(TOPIC_NAME))
    .withView("event.output", EVENTS_DS_URI)
    .build();

stream(streamDescription);
```

The complete streaming example is available in the kite-apps-examples project.

## Installation
This library has two parts that must be installed: Oozie URI support for Kite datasets and a command-line tool for deploying applications.

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

## Running the scheduled job examples
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

## Running the streaming job example
The streaming job pulls data from a Kafka queue and writes it to a Kite dataset. The application will create the Kafka queue and dataset if it doesn't already exist. For this to run we need to configure the application to use a Kafka installation. This is done by creating a properties file like this:

```properties
kafka.zookeeper.connect=example.zookeeper.host:2181
kafka.metadata.broker.list=example.broker.host1:9092,example.broker.host2:9092
```
Any property defined by Kafka can be set by simply having a "kafka." prefix here. (Similarly, any Spark property can be defined in this file as well.)

Once the example.properties file exists, we can install the application with this command:

```bash
bin/kite-apps install /path/to/example/target/kite-apps-examples-0.1.0-SNAPSHOT.jar \
org.kitesdk.apps.examples.streaming.SparkStreamingApp \
/path/to/install/dir/on/hdfs \
--properties-file /path/to/example/example.properties
```

This will install the application to the given directory and create the Spark streaming job.

The example includes a simple command to generate data and write it to the application's Spark topic, which can be run like this:

```bash
bin/kite-apps jar /path/to/example/target/kite-apps-examples-0.1.0-SNAPSHOT.jar \
org.kitesdk.apps.examples.streaming.DataGenerator <kafka.broker.list> example_events
```

As data is written, the running example application should consume it and write output to the ```example/sparkevents``` dataset.

**Note for CDH 5.4 Users:** CDH 5.4.x will have a version of Kite 1.0.x in the /usr/jars folder, which will appear on the classpath of the Spark Streaming executor. This conflicts with the 1.1.0 Kite Jars used by this project. It may be necessary to replace the Kite 1.0 JARS in /usr/jars with their kite 1.1 equivalents. This will be unnecessary in later versions of CDH that upgrade to Kite 1.1 or newer.

## Configuration
All application configuration is in an app.properties file that can be provided when the application is installed. The property names use the following structure:

#### Application-wide configuration
Everything from app.properties is visible in the AppContext class provided to applications at runtime.

#### Job-specific configuration
Configuration specific to jobs take the form ```kite.job.<job-name>.<setting>```. The job name is the string returned by the getName() method on the job implementation. For instance, the following line can will configure the Spark executor memory for a job:

```properties
kite.job.my-example-job.spark.executor.memory=1g
```

#### Job input and output configuration
Configuration specific to named job inputs and outputs live under the an ```kite.job.<job-name>.input.<input-name>``` and ```kite.job.<job-name>.output.<output-name>``` key of jobs. For instance, the following setting is used to point an input to a specific Kafka topic:

```properties
kite.job.my-example-job.input.my-kafka-input.kite.topic=example_topic
```

Properties to be passed to the underlying data provider, such as Kafka, can be specified with a kafka prefix. For instance, the Kafka consumer group used for a job input can be specified like this:

```properties
kite.job.my-example-job.input.my-kafka-input.kafka.group.id=my_group_id
````

Anything after the kafka prefix is simply passed to the underlying Kafka consumer or producer configuration, for job inputs and outputs respectively.

#### Customized Hadoop configuration
The ```hadoop``` prefix is used to customize Hadoop configuration in applications and jobs. It can be set as an application-wide settings or at a job-specific setting. For instance, use the following to set ```mapreduce.map.memory.mb``` for the entire application and ```mapreduce.reduce.memory.mb``` for a single job within that application:

```properties
hadoop.mapreduce.map.memory.mb=2048
kite.job.my-example-job.hadoop.mapreduce.reduce.memory.mb=4096
```

These settings will be used as expected in the Hadoop configuration for the job. Job-specific settings will override settings at the application level if they are defined in both places.

#### Spark Configuration
The ```spark``` prefix is used to customize Spark configuration in applications and jobs. For instance, to set the Spark executor memory for the entire application and RDD compression for a specific job, the following can be done:

```properties
spark.executor.memory=2g
kite.job.my-example-job.spark.rdd.compress=true
```

Since Spark configuration settings already start with a "spark." prefix, we do not require an additional prefix to identify them. So the setting names are ```spark.executor.memory``` rather than ```spark.spark.executor.memory```.

#### Kafka Configuration
The ```kafka``` prefix is used to customize Kafka configuration in applications and jobs. For instance, to set the Kafka broker list and Zookeeper settings across the application and override the timeout for a specific job:

```properties
kafka.metadata.broker.list=host1:9092,host2:9092
kafka.zookeeper.connect=host3:2181,host4:2181,host5:2181
kite.job.my-example-job.kafka.socket.timeout.ms=5000
```

## Kite Apps layout
Kite Applications are installed to a target directory, which contains the following structure:

```
<app-root>/conf -- The app.properties file, and a place for future configuration.
<app-root>/streaming -- JSON files containing descriptions of each streaming job.
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
* Enhance interpretation of conf/app.properties for job-specific configuration and for well-defined configuration of subsystems like Spark and Kafka. See [issue 3](https://github.com/rbrush/kite-apps/issues/3).
* Tooling to simplify upgrades and uninstallation of applications. See [issue 10](https://github.com/rbrush/kite-apps/issues/10).
* Spark Streaming jobs must checkpoint their state to recover from failures. See [issue 12](https://github.com/rbrush/kite-apps/issues/12).
