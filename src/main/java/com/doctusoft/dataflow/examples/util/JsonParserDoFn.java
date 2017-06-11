package com.doctusoft.dataflow.examples.util;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;

/**
 * Created by cskassai on 2017. 06. 05..
 */
@Slf4j
@RequiredArgsConstructor()
public class JsonParserDoFn<T> extends DoFn<String, T> {
    
    private Gson gson;
    
    private final Class<T> classOfT;
    
    public static <T> JsonParserDoFn<T> of(Class<T> classOfT) {
        return new JsonParserDoFn<>(classOfT);
    }
    
    @Setup
    public void setup() {
        gson = new GsonBuilder().registerTypeAdapter(DateTime.class, DateTimeGsonAdapter.create()).create();
    }
    
    @ProcessElement
    public void processElement(ProcessContext context) {
        String json = context.element();
        T t = null;
        try {
            t = gson.fromJson(json, classOfT);
            
        } catch (Exception e) {
            log.error(String.format("Error parsing string %s", json), e);
        }
        if(t != null) {
            context.output(t);
        }
    }
}
