package com.doctusoft.dataflow.examples;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.Lists;
import lombok.extern.log4j.Log4j2;

import java.util.UUID;

/**
 * Created by cskassai on 2017. 01. 18.
 */
@Log4j2
public class PubsubUUIDSender {

    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();



    public static void main(String... args) throws Exception {

        GoogleCredential credential = GoogleCredential.getApplicationDefault(HTTP_TRANSPORT, JSON_FACTORY)
                                                        .createScoped(PubsubScopes.all());


        Pubsub pubsub = new Pubsub.Builder( HTTP_TRANSPORT, JSON_FACTORY, credential)
                                    .setApplicationName(PubsubUUIDSender.class.getSimpleName())
                                    .build();

        while(true) {
                PubsubMessage message = new PubsubMessage().encodeData(UUID.randomUUID().toString().getBytes("UTF-8"));
                PublishRequest request = new PublishRequest();
                request.setMessages(Lists.newArrayList(message));
                PublishResponse response = pubsub.projects().topics().publish("projects/" + CustomWindowingPipeline.PROJECT + "/topics/" + CustomWindowingPipeline.PUBSUB_TOPIC_ID, request).execute();
                log.info("Response: {}", response);
                Thread.sleep(100);
        }
    }



}
