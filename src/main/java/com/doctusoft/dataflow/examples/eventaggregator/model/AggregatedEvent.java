package com.doctusoft.dataflow.examples.eventaggregator.model;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/**
 * Created by cskassai on 2017. 06. 05..
 */
@Value
@Builder
@DefaultCoder(SerializableCoder.class)
public class AggregatedEvent implements Serializable {
    
    public static final String CUSTOMER_ID = "customerId";
    public static final String TIMESTAMP = "timestamp";
    public static final String SENDS = "sends";
    public static final String OPENS = "opens";
    public static final String CREATED_AT = "created_at";
    
    
    private String customerId;
    private DateTime timestamp;
    private int sends;
    private int opens;
    
    public static SerializableFunction<AggregatedEvent, TableRow> formatFunction() {
        return (AggregatedEvent e) -> {
            TableRow tableRow = new TableRow();
            tableRow.set(CUSTOMER_ID, e.getCustomerId());
            tableRow.set(TIMESTAMP, e.getTimestamp().toInstant().getMillis()/1000);
            tableRow.set(SENDS, e.getSends());
            tableRow.set(OPENS, e.getOpens());
            tableRow.set(CREATED_AT, Instant.now().getMillis()/1000);
            return tableRow;
        };
    }
    
    public static TableSchema bqSchema() {
        TableSchema tableSchema = new TableSchema();
        TableFieldSchema idField = new TableFieldSchema();
        idField.setName(CUSTOMER_ID);
        idField.setType("STRING");
        
        TableFieldSchema timestampField = new TableFieldSchema();
        timestampField.setName(TIMESTAMP);
        timestampField.setType("TIMESTAMP");
    
        TableFieldSchema sendsFields = new TableFieldSchema();
        sendsFields.setName(SENDS);
        sendsFields.setType("INTEGER");
    
        TableFieldSchema opensField = new TableFieldSchema();
        opensField.setName(OPENS);
        opensField.setType("INTEGER");
    
        TableFieldSchema createdAtField = new TableFieldSchema();
        createdAtField.setName(CREATED_AT);
        createdAtField.setType("TIMESTAMP");
        
        tableSchema.setFields(Lists.newArrayList(idField, timestampField, sendsFields, opensField, createdAtField));
        
        return tableSchema;
    }
}
