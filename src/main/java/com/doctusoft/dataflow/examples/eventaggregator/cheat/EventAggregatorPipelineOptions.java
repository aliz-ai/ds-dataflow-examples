package com.doctusoft.dataflow.examples.eventaggregator.cheat;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * Created by cskassai on 2017. 06. 05..
 */
public interface EventAggregatorPipelineOptions extends DataflowPipelineOptions {
    
    @Default.String("bdf_events")
    String getBigqueryDatasetId();
    void setBigqueryDatasetId(String bigqueryDatasetId);
    
    @Validation.Required
    String getSourceTopic();
    void setSourceTopic(String sourceTopic);
    
    
    
}
