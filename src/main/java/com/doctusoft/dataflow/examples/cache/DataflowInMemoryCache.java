package com.doctusoft.dataflow.examples.cache;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

/**
 * Created by cskassai on 2017. 05. 05..
 */
@RequiredArgsConstructor
@Slf4j
public class DataflowInMemoryCache {
    
    private final Map<Object, CachedObject> cache = Maps.newHashMap();
    
    private final Map<Object, CacheConfiguration> configuration;
    
    public static DataflowInMemoryCacheBuilder builder() {
        return new DataflowInMemoryCacheBuilder();
    }
    
    public <O> O get(Object key) {
        CachedObject<O> cachedObject = this.cache.get(key);
        CacheConfiguration<O> cacheConfiguration = this.configuration.get(key);
        Preconditions.checkNotNull(cacheConfiguration, "No configuration for key %s", key);
        if (cachedObject == null || cacheConfiguration.refreshPredicate.test(cachedObject)) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            log.info("Reloading cache for key: {}", key);
            O o = cacheConfiguration.getSupplier().get();
            this.cache.put(key, CachedObject.of(o));
            log.info("Reload took {} for key {}", stopwatch.stop(), key);
            return o;
        } else {
            return cachedObject.get();
        }
    }
    
    @Value
    public static class CachedObject<O> extends SoftReference<O> implements Serializable {
        
        private LocalDateTime loadedAt;
        
        public CachedObject(O object, LocalDateTime loadedAt) {
            super(object);
            this.loadedAt = loadedAt;
        }
        
        public static <O> CachedObject<O> of(O object) {
            return new CachedObject<>(object, LocalDateTime.now());
        }
        
    }
    
    @Value
    @Builder
    public static class CacheConfiguration<O> implements Serializable {
        
        private Supplier<O> supplier;
        private Predicate<CachedObject<O>> refreshPredicate;
    }
    
    public static class DataflowInMemoryCacheBuilder {
        
        private final Map<Object, CacheConfiguration> configurationMap = Maps.newHashMap();
        
        public DataflowInMemoryCacheBuilder addConfiguration(Object key, CacheConfiguration configuration) {
            this.configurationMap.put(key, configuration);
            
            return this;
        }
        
        public DataflowInMemoryCache build() {
            return new DataflowInMemoryCache(this.configurationMap);
        }
    }
    
    
    
}
