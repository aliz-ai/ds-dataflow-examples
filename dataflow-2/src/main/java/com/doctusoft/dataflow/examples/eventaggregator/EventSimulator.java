package com.doctusoft.dataflow.examples.eventaggregator;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.doctusoft.dataflow.examples.eventaggregator.model.Event;
import com.doctusoft.dataflow.examples.util.DateTimeGsonAdapter;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.spi.v1.Publisher;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import org.joda.time.DateTime;

/**
 * Created by cskassai on 2017. 06. 11..
 */
@Slf4j
public class EventSimulator {
    
    public static final String PROJECT = "...";
    public static final String TOPIC = "...";
    
    public static void main(String[] args) throws Exception {
        
        Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, DateTimeGsonAdapter.create()).create();
        
        Publisher publisher = Publisher.defaultBuilder(TopicName.create(PROJECT, TOPIC)).build();
        
        while (true) {
    
            int delayInSeconds = (int)((Math.random()) * 240);
            Event.EventBuilder event = Event.builder()
                                            .customerId(UUID.randomUUID().toString().substring(0, 1))
                                            .id(UUID.randomUUID().toString())
                                            .timestamp(DateTime.now().minusHours(2).minusSeconds(delayInSeconds))
                                            .type(Math.random() > 0.8 ? Event.EventType.SEND : Event.EventType.OPEN);
    
            String json = gson.toJson(event);
            ApiFuture<String> publish = publisher.publish(PubsubMessage.newBuilder().setData(ByteString.copyFrom(json, StandardCharsets.UTF_8)).build());
    
            log.info("Event {} sended, id: {}", json, publish.get());
            
            Thread.sleep(200);
        }
    }
}
