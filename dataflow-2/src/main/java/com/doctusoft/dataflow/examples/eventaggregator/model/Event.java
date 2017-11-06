package com.doctusoft.dataflow.examples.eventaggregator.model;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Wither;

import java.io.Serializable;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/**
 * Created by cskassai on 2017. 06. 05..
 */
@Builder
@Value
public class Event implements Serializable {
    
    public static final String ID = "id";
    public static final String CUSTOMER_ID = "customerId";
    public static final String TIMESTAMP = "timestamp";
    public static final String PROCESSING_TIME = "processing_time";
    public static final String CREATED_AT = "created_at";
    public static final String TYPE = "type";
    
    
    private String id;
    private DateTime timestamp;
    private EventType type;
    private String customerId;
    
    @Wither
    private Instant processingTime;
    
    public enum EventType {
        SEND,
        OPEN
        
    }
    
    
    public static SerializableFunction<Event, TableRow> formatFunction() {
        return (Event e) -> {
            TableRow tableRow = new TableRow();
            tableRow.set(ID, e.getId());
            tableRow.set(CUSTOMER_ID, e.getCustomerId());
            tableRow.set(TIMESTAMP, e.getTimestamp().toInstant().getMillis()/1000);
            tableRow.set(CREATED_AT, Instant.now().getMillis()/1000);
            tableRow.set(PROCESSING_TIME, e.getProcessingTime().getMillis()/1000);
            tableRow.set(TYPE, e.getType());
            return tableRow;
        };
    }
    
    public static TableSchema bqSchema() {
        TableSchema tableSchema = new TableSchema();
        TableFieldSchema idField = new TableFieldSchema();
        idField.setName(ID);
        idField.setType("STRING");
    
        TableFieldSchema customerIdField = new TableFieldSchema();
        customerIdField.setName(CUSTOMER_ID);
        customerIdField.setType("STRING");
        
        TableFieldSchema timestampField = new TableFieldSchema();
        timestampField.setName(TIMESTAMP);
        timestampField.setType("TIMESTAMP");
        
        TableFieldSchema typeField = new TableFieldSchema();
        typeField.setName(TYPE);
        typeField.setType("STRING");
    
        TableFieldSchema createdAtField = new TableFieldSchema();
        createdAtField.setName(CREATED_AT);
        createdAtField.setType("TIMESTAMP");
    
        TableFieldSchema processingTimeField = new TableFieldSchema();
        processingTimeField.setName(PROCESSING_TIME);
        processingTimeField.setType("TIMESTAMP");
        
        tableSchema.setFields(Lists.newArrayList(idField, customerIdField, timestampField, typeField, createdAtField, processingTimeField));
        
        return tableSchema;
    }
}
