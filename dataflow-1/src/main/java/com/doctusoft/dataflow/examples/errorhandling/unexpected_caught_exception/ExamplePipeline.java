package com.doctusoft.dataflow.examples.errorhandling.unexpected_caught_exception;

import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ContiguousSet;

@Slf4j
public class ExamplePipeline {
    
    public static void main(String[] args) {
        
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
        
        options.setProject("merkur-developement");
        options.setRunner(DataflowPipelineRunner.class);
        options.setNumWorkers(1);
        options.setZone("europe-west1-d");
        options.setWorkerMachineType("n1-standard-1");
        //options.setStagingLocation("gs://example-bucket/staging/ExamplePipeline/");
        options.setStagingLocation("gs://merkur-developement-dataflow/staging/ExamplePipeline/");
        options.setTempLocation("gs://merkur-developement-dataflow/staging/ExamplePipeline/");
        
        Iterable<Integer> values = ContiguousSet.closed(0, 10);
        
        Pipeline p = Pipeline.create(options);
        
        PCollection<Integer> init = p.apply(Create.of(values));
        PCollection<KV<Integer, Integer>> times16 = init.apply(WithKeys.of(i -> i)).setCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()))
                                                        .apply(ParDo.of(new Multiply1(2)))
                                                        .apply(ParDo.of(new Multiply2(2)))
                                                        .apply(ParDo.of(new Multiply3(2)))
                                                        .apply(ParDo.of(new Multiply4(2)));
        
        times16.apply(ParDo.of(FinalizerError.of()));
        
        p.run();
    }
    
    @RequiredArgsConstructor
    private static abstract class Multiply extends DoFn<KV<Integer, Integer>, KV<Integer, Integer>> {
        
        private final int operand;
        
        @Override
        public void processElement(ProcessContext c) throws Exception {
            try {
                KV<Integer, Integer> input = c.element();
                if (input != null) {
                    input = KV.of(input.getKey(), input.getValue() * operand);
                }
                c.output(input);
            } catch (Exception e) {
                log.error(String.format("Error in transform %s", this.getClass().getSimpleName()), e);
            }
        }
    }
    
    private static class Multiply1 extends Multiply {
        
        Multiply1(int operand) { super(operand); }
        
        @Override
        public void processElement(ProcessContext c) throws Exception { super.processElement(c); }
    }
    
    private static class Multiply2 extends Multiply {
        
        Multiply2(int operand) { super(operand); }
        
        @Override
        public void processElement(ProcessContext c) throws Exception { super.processElement(c); }
    }
    
    private static class Multiply3 extends Multiply {
        
        Multiply3(int operand) { super(operand); }
        
        @Override
        public void processElement(ProcessContext c) throws Exception { super.processElement(c); }
    }
    
    private static class Multiply4 extends Multiply {
        
        Multiply4(int operand) { super(operand); }
        
        @Override
        public void processElement(ProcessContext c) throws Exception { super.processElement(c); }
    }
    
    @NoArgsConstructor(staticName = "of")
    private static class FinalizerError extends DoFn<KV<Integer, Integer>, KV<Integer, Integer>> {
        
        @Override
        public void processElement(ProcessContext c) throws Exception {
            KV<Integer, Integer> input = c.element();
            log.info("Final value: {}", input);
            if (input == null || input.getValue() == 0) {
                // explicitly fail if there is invalid data
                throw new RuntimeException("Zero or missing value in Pipeline!");
            }
            c.output(input);
        }
    }
}
