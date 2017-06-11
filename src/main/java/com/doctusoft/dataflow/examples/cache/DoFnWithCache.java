package com.doctusoft.dataflow.examples.cache;

import java.time.LocalDateTime;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Created by cskassai on 2017. 05. 26..
 */
public class DoFnWithCache extends DoFn<String, String> {
    
    public static final String FILTER_KEY = "key";
    private DataflowInMemoryCache dataflowInMemoryCache;
    
    @Setup
    public void setup() throws Exception {
        dataflowInMemoryCache = DataflowInMemoryCache.builder()
                                                     .addConfiguration(FILTER_KEY,
                                                                       DataflowInMemoryCache.CacheConfiguration.builder()
                                                                                                               .refreshPredicate((e) -> e.getLoadedAt()
                                                                                                                                         .isBefore(LocalDateTime.now()
                                                                                                                                                                .minusMinutes(5l)))
                                                                                                               .supplier(() -> Sets.newHashSet("key1", "key2"))
                                                                                                               .build())
                                                     .build();
    }
    
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        
        Set<String> dataToFilter = dataflowInMemoryCache.get(FILTER_KEY);
        
        String element = c.element();
        if (dataToFilter.contains(element)) {
            c.output(element);
        }
        
    }
}
