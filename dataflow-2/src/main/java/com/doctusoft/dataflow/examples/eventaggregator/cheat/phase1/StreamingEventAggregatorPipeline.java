package com.doctusoft.dataflow.examples.eventaggregator.cheat.phase1;

import lombok.extern.slf4j.Slf4j;

import com.doctusoft.dataflow.examples.eventaggregator.cheat.EventAggregatorPipelineOptions;
import com.doctusoft.dataflow.examples.eventaggregator.model.Event;
import com.doctusoft.dataflow.examples.util.JsonParserDoFn;
import com.google.api.services.bigquery.model.TableReference;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

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
        
        pipeline.apply(PubsubIO.readStrings()
                               .fromTopic(String.format("projects/%s/topics/%s", options.getProject(), options.getSourceTopic())))
                .apply(ParDo.of(JsonParserDoFn.of(Event.class))).setCoder(SerializableCoder.of(Event.class))
                .apply(ParDo.of(new DoFn<Event, Event>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        Event eventWithProcessingTime = context.element().withProcessingTime(context.timestamp());
                        context.output(eventWithProcessingTime);
                    }
                }))
                .apply(BigQueryIO.<Event>write()
                               .to(rawEventtable)
                               .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                               .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                               .withFormatFunction(Event.formatFunction())
                               .withSchema(Event.bqSchema()));
        
        pipeline.run();
        
    }
}
