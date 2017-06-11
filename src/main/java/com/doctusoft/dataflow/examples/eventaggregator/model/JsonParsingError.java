package com.doctusoft.dataflow.examples.eventaggregator.model;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

/**
 * Created by cskassai on 2017. 06. 11..
 */
@Value
@Builder
public class JsonParsingError implements Serializable {
    
    public static final String CREATED_AT = "created_at";
    public static final String JSON = "json";
    public static final String EXCEPTION_CLASS = "exception_class";
    public static final String ERROR_MESSAGE = "error_message";
    
    String errorMessage;
    String exceptionClass;
    String json;
    
    
    public static SerializableFunction<JsonParsingError, TableRow> formatFunction() {
        return (JsonParsingError e) -> {
            TableRow tableRow = new TableRow();
            tableRow.set(ERROR_MESSAGE, e.getErrorMessage());
            tableRow.set(EXCEPTION_CLASS, e.getExceptionClass());
            tableRow.set(JSON, e.getJson());
            tableRow.set(CREATED_AT, Instant.now().getMillis()/1000);
            return tableRow;
        };
    }
    
    public static TableSchema bqSchema() {
        TableSchema tableSchema = new TableSchema();
        TableFieldSchema errorMessageField = new TableFieldSchema();
        errorMessageField.setName(ERROR_MESSAGE);
        errorMessageField.setType("STRING");
        
        TableFieldSchema exceptionClassField = new TableFieldSchema();
        exceptionClassField.setName(EXCEPTION_CLASS);
        exceptionClassField.setType("STRING");
        
        TableFieldSchema jsonField = new TableFieldSchema();
        jsonField.setName(JSON);
        jsonField.setType("STRING");
        
        TableFieldSchema createdAtField = new TableFieldSchema();
        createdAtField.setName(CREATED_AT);
        createdAtField.setType("TIMESTAMP");
        
        tableSchema.setFields(Lists.newArrayList(errorMessageField, exceptionClassField, jsonField, createdAtField));
        
        return tableSchema;
    }
    
}
