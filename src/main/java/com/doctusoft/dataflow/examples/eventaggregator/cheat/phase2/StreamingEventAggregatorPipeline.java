package com.doctusoft.dataflow.examples.eventaggregator.cheat.phase2;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.doctusoft.dataflow.examples.eventaggregator.cheat.EventAggregatorPipelineOptions;
import com.doctusoft.dataflow.examples.eventaggregator.model.AggregatedEvent;
import com.doctusoft.dataflow.examples.eventaggregator.model.Event;
import com.doctusoft.dataflow.examples.util.JsonParserDoFn;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.collect.Streams;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * Created by cskassai on 2017. 06. 05..
 */
@Slf4j
public class StreamingEventAggregatorPipeline {
    
    public static void main(String[] args) {
        
        EventAggregatorPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(EventAggregatorPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        
        TableReference rawEventtable = new TableReference();
        rawEventtable.setProjectId(options.getProject());
        rawEventtable.setDatasetId(options.getBigqueryDatasetId());
        rawEventtable.setTableId(Event.class.getSimpleName().toLowerCase());
        
        TableReference aggregatedEventtable = new TableReference();
        aggregatedEventtable.setProjectId(options.getProject());
        aggregatedEventtable.setDatasetId(options.getBigqueryDatasetId());
        aggregatedEventtable.setTableId(AggregatedEvent.class.getSimpleName().toLowerCase());
        
        PCollection<Event> events = pipeline.apply(PubsubIO.readStrings()
                                                           .fromTopic(String.format("projects/%s/topics/%s", options.getProject(), options.getSourceTopic())))
                                            .apply(ParDo.of(JsonParserDoFn.of(Event.class))).setCoder(SerializableCoder.of(Event.class))
                                            .apply(ParDo.of(new DoFn<Event, Event>() {
                                                @ProcessElement
                                                public void processElement(ProcessContext context) {
                                                    Event eventWithProcessingTime = context.element().withProcessingTime(context.timestamp());
                                                    context.output(eventWithProcessingTime);
                                                }
                                            }))
                                            .apply(WithTimestamps.of((Event e) -> e.getTimestamp().toInstant()).withAllowedTimestampSkew(Duration.standardMinutes(10)));
        events.apply(BigQueryIO.<Event>write()
                             .to(rawEventtable)
                             .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                             .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                             .withFormatFunction(Event.formatFunction())
                             .withSchema(Event.bqSchema()));
        
        events.apply(Window.<Event>into(FixedWindows.of(Duration.standardHours((1))))
                             .triggering(AfterWatermark.pastEndOfWindow()
                                                       .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                                                            .plusDelayOf(Duration.standardSeconds(5)))
                                                       .withLateFirings(AfterPane.elementCountAtLeast(1)))
                             .withAllowedLateness(Duration.standardHours(1))
                             .discardingFiredPanes())
              .apply(WithKeys.of(Event::getCustomerId)).setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Event.class)))
              .apply(GroupByKey.create())
              .apply(ParDo.of(new DoFn<KV<String, Iterable<Event>>, AggregatedEvent>() {

                  @ProcessElement
                  public void processElement(ProcessContext context, BoundedWindow boundedWindow) {
                      KV<String, Iterable<Event>> element = context.element();
                      AggregatedEvent aggregatedEvent = null;
                      try {
                          Map<Event.EventType, List<Event>> eventsByType = Streams.stream(element.getValue()).collect(Collectors.groupingBy(Event::getType));
    
                          List<Event> sendEvents = eventsByType.get(Event.EventType.SEND);
                          List<Event> openEvents = eventsByType.get(Event.EventType.OPEN);
                          aggregatedEvent = AggregatedEvent.builder()
                                                           .customerId(element.getKey())
                                                           .sends(sendEvents == null ? 0 : sendEvents.size())
                                                           .opens(openEvents == null ? 0 : openEvents.size())
                                                           .timestamp(new DateTime(boundedWindow.maxTimestamp()))
                                                           .build();

                      } catch (Exception e) {
                          log.error(String.format("Error processing key %s", element.getKey()), e);
                      }

                      if (aggregatedEvent != null) {
                          context.output(aggregatedEvent);
                      }

                  }
              }))
              .apply(BigQueryIO.<AggregatedEvent>write()
                             .to(aggregatedEventtable)
                             .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                             .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                             .withFormatFunction(AggregatedEvent.formatFunction())
                             .withSchema(AggregatedEvent.bqSchema()));
        
        pipeline.run();
        
    }
}
