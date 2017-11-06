package com.doctusoft.dataflow.examples.eventaggregator;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Created by cskassai on 2017. 06. 11..
 */
public class TestPipeline {
    
    private final static String PROJECT = "...";
    private final static String TOPIC = "bdf_events";
    private final static String BQ_DATASET = "bdf_events";
    public static final String DATA = "data";
    
    public static void main(String[] args) {
        
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setProject(PROJECT);
        
        TableSchema tableSchema = new TableSchema().setFields(Lists.newArrayList(new TableFieldSchema().setName(DATA).setType("STRING")));
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        pipeline.apply(PubsubIO.readStrings().fromTopic(String.format("projects/%s/topics/%s", PROJECT, TOPIC)))
                .apply(BigQueryIO.<String>write()
                               .withSchema(tableSchema)
                               .withFormatFunction((String s) -> {
                                   TableRow tableRow = new TableRow();
                                   tableRow.set(DATA, s);
                                   return tableRow;
                               })
                               .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                               .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                               .to(new TableReference().setProjectId(
                                       PROJECT).setDatasetId(BQ_DATASET).setTableId("test")));
        
        
        pipeline.run();
        
    }
}
