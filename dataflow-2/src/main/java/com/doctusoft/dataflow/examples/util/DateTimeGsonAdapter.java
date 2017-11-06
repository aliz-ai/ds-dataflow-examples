package com.doctusoft.dataflow.examples.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by cskassai on 2017. 06. 05..
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DateTimeGsonAdapter extends TypeAdapter<DateTime> {
    
    public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    
    public static TypeAdapter<DateTime> create() {
        return new DateTimeGsonAdapter().nullSafe();
    }
    
    @Override
    public void write(JsonWriter out, DateTime value) throws IOException {
        out.value(DATE_TIME_FORMAT.print(value));
    }
    
    @Override
    public DateTime read(JsonReader in) throws IOException {
        return DATE_TIME_FORMAT.parseDateTime(in.nextString());
    }
}
