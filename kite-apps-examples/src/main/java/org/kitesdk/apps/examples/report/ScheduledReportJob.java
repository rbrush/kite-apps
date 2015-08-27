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
package org.kitesdk.apps.examples.report;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.types.avro.Avros;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.kitesdk.apps.crunch.AbstractCrunchJob;
import org.kitesdk.apps.example.event.ExampleEvent;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;

import static org.kitesdk.apps.examples.report.ScheduledReportApp.SCHEMA;

public class ScheduledReportJob extends AbstractCrunchJob {

  public static class GetEventId extends MapFn<ExampleEvent, Long> {

    private static final long serialVersionUID = 1;

    @Override
    public Long map(ExampleEvent exampleEvent) {
      return exampleEvent.getUserId();
    }
  }

  public static class ToUserReport extends MapFn<Pair<Long, Long>, GenericData.Record> {

    private static final long serialVersionUID = 1;

    @Override
    public GenericData.Record map (Pair < Long, Long > pair){

      GenericData.Record record = new GenericData.Record(SCHEMA);

      record.put("user_id", pair.first());
      record.put("event_count", pair.second());

      return record;
    }
  };

  public void run() {

    // TODO: Switch to parameterized views.
    View<ExampleEvent> view = Datasets.load(ScheduledReportApp.EXAMPLE_DS_URI,
        ExampleEvent.class);

    RefinableView<GenericRecord> target = Datasets.load(ScheduledReportApp.REPORT_DS_URI,
        GenericRecord.class);

    // Get the view into which this report will be written.
    DateTime dateTime = getNominalTime().toDateTime(DateTimeZone.UTC);

    View<GenericRecord> output = target
        .with("year", dateTime.getYear())
        .with("month", dateTime.getMonthOfYear())
        .with("day", dateTime.getDayOfMonth())
        .with("hour", dateTime.getHourOfDay())
        .with("minute", dateTime.getMinuteOfHour());

    Pipeline pipeline = getPipeline();

    PCollection<ExampleEvent> events = pipeline.read(CrunchDatasets.asSource(view));

    PTable<Long, ExampleEvent> eventsByUser = events.by(new GetEventId(), Avros.longs());

    // Count of events by user ID.
    PTable<Long, Long> userEventCounts = eventsByUser.keys().count();

    PCollection<GenericData.Record> report = userEventCounts.parallelDo(
        new ToUserReport(),
        Avros.generics(SCHEMA));

    pipeline.write(report, CrunchDatasets.asTarget(output));

    pipeline.run();
  }
}
