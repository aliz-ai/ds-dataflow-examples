package com.doctusoft.dataflow.examples.cache;

import java.time.LocalDateTime;
import java.util.Set;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.common.collect.Sets;

/**
 * Created by cskassai on 2017. 05. 26..
 */
public class DoFnWithCache extends DoFn<String, String> {
    
    public static final String FILTER_KEY = "key";
    private DataflowInMemoryCache dataflowInMemoryCache;
    
    @Override
    public void startBundle(Context c) throws Exception {
        super.startBundle(c);
        dataflowInMemoryCache = DataflowInMemoryCache.builder()
                                                     .addConfiguration(FILTER_KEY,
                                                                       DataflowInMemoryCache.CacheConfiguration.builder()
                                                                                                               .refreshPredicate((e) -> e.getLoadedAt()
                                                                                                                                         .isBefore(LocalDateTime.now()
                                                                                                                                                                .minusMinutes(5l)))
                                                                                                               .supplier(() -> Sets.newHashSet("alma", "korte"))
                                                                                                               .build())
                                                     .build();
    }
    
    @Override
    public void processElement(ProcessContext c) throws Exception {
        
        Set<String> dataToFilter = dataflowInMemoryCache.get(FILTER_KEY);
        
        String element = c.element();
        if (dataToFilter.contains(element)) {
            c.output(element);
        }
        
    }
}
