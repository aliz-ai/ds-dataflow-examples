package com.doctusoft.dataflow.examples;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DurationCoder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * Created by cskassai on 2017. 01. 18.
 */
public class CustomWindowingPipeline {

    public static final String DATA_COLUMN_NAME = "data";
    public static final String PUBSUB_TOPIC_ID = "custom_windowing";
    public static final String BQ_TABLE_PREFIX = "data";
    public static final String BQ_DATASET = "custom_windowing";
    public static final String PROJECT = "...";
    public static final String STAGING_LOCATION = "gs://...";

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        options.setStreaming(true);
        options.setNumWorkers(1);
        options.setRunner(DataflowPipelineRunner.class);
        options.setStagingLocation(STAGING_LOCATION);
        options.setProject(PROJECT);

        TableSchema tableSchema = new TableSchema();
        TableFieldSchema dataColumnSchema = new TableFieldSchema();
        dataColumnSchema.setName(DATA_COLUMN_NAME);
        dataColumnSchema.setType("STRING");
        tableSchema.setFields(Lists.newArrayList(dataColumnSchema));

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> windowedIds = pipeline.apply(PubsubIO.Read.topic("projects/" + PROJECT + "/topics/" + PUBSUB_TOPIC_ID))
                                            .<PCollection<String>>apply(Window.into(
                                                    FixedKeyedWindowingFunction.of(
                                                            Duration.standardMinutes(5l),
                                                            new CustomerIdGeneratorFunction())));

        windowedIds.apply(ParDo.of(new DoFn<String, TableRow>() {
                                    @Override
                                    public void processElement(ProcessContext c) throws Exception {
                                        String element = c.element();
                                        TableRow tableRow = new TableRow();
                                        tableRow.set(DATA_COLUMN_NAME, element);
                                        c.output(tableRow);
                                    }}))
                    .apply(BigQueryIO.Write.to(new TableNameFunction(PROJECT, BQ_DATASET, BQ_TABLE_PREFIX))
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withSchema(tableSchema));


        pipeline.run();
    }

    static class CustomerIdGeneratorFunction implements SerializableFunction<String, String> {

        public String apply(String input) {
            return input.substring(0, 1);
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    static class TableNameFunction implements SerializableFunction<BoundedWindow, String> {

        private String qualifiedTableName;

        public TableNameFunction(String project, String dataset, String tableNamePrefix){
            Preconditions.checkArgument(!Strings.isNullOrEmpty(project));
            Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset));
            Preconditions.checkArgument(!Strings.isNullOrEmpty(tableNamePrefix));

            this.qualifiedTableName = project + ":" + dataset + "." + tableNamePrefix;
        }

        public String apply(BoundedWindow window) {
            Preconditions.checkArgument((window instanceof FixedKeyedWindow), "This naming can be used only with FixedKeyedWindow");

            String key = ((FixedKeyedWindow)window).key();
            return qualifiedTableName + "_" + key;
        }
    }


    public static class FixedKeyedWindowingFunction<T> extends NonMergingWindowFn<T, FixedKeyedWindow> {


        private final SerializableFunction<T, String> idWindowingFunction;

        /**
         * Size of this window.
         */
        private final Duration size;

        /**
         * Offset of this window.  Windows start at time
         * N * size + offset, where 0 is the epoch.
         */
        private final Duration offset;

        /**
         * Partitions the timestamp space into half-open intervals of the form
         * [N * size, (N + 1) * size), where 0 is the epoch.
         */
        public static <T> FixedKeyedWindowingFunction of(Duration size, SerializableFunction<T, String> idWindowingFunction) {
            return new FixedKeyedWindowingFunction(idWindowingFunction, size, Duration.ZERO);
        }

        /**
         * Partitions the timestamp space into half-open intervals of the form
         * [N * size + offset, (N + 1) * size + offset),
         * where 0 is the epoch.
         *
         * @throws IllegalArgumentException if offset is not in [0, size)
         */
        public FixedKeyedWindowingFunction withOffset(Duration offset) {
            return new FixedKeyedWindowingFunction(idWindowingFunction, size, offset);
        }

        private FixedKeyedWindowingFunction(SerializableFunction<T, String> idWindowingFunction, Duration size, Duration offset) {
            if (offset.isShorterThan(Duration.ZERO) || !offset.isShorterThan(size)) {
                throw new IllegalArgumentException(
                        "FixedWindows WindowingStrategies must have 0 <= offset < size");
            }
            this.size = size;
            this.offset = offset;
            this.idWindowingFunction = idWindowingFunction;
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("size", size)
                        .withLabel("Window Duration"))
                    .addIfNotDefault(DisplayData.item("offset", offset)
                        .withLabel("Window Start Offset"), Duration.ZERO)
                    .add(DisplayData.item("windowingfunction", idWindowingFunction.toString())
                        .withLabel("Windowing Function"));
        }

        @Override
        public Coder<FixedKeyedWindow> windowCoder() {
            return FixedKeyedWindow.FixedKeyedWindowCoder.of();
        }

        @Override
        public boolean isCompatible(WindowFn<?, ?> other) {
            return this.equals(other);
        }

        public Duration getSize() {
            return size;
        }

        public Duration getOffset() {
            return offset;
        }

        public SerializableFunction<T, String> getIdWindowingFunction() {
            return idWindowingFunction;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof FixedKeyedWindowingFunction)) {
                return false;
            }
            FixedKeyedWindowingFunction other = (FixedKeyedWindowingFunction) object;
            return getOffset().equals(other.getOffset())
                    && getSize().equals(other.getSize())
                    && getIdWindowingFunction().equals(other.getIdWindowingFunction());
        }

        @Override
        public int hashCode() {
            return Objects.hash(size, offset, idWindowingFunction);
        }


        @Override
        public final Collection<FixedKeyedWindow> assignWindows(AssignContext c) {

            Instant timestamp = c.timestamp();
            long start = timestamp.getMillis()
                    - timestamp.plus(size).minus(offset).getMillis() % size.getMillis();

            T element = c.element();
            String key = idWindowingFunction.apply(element);

            return Arrays.asList(new FixedKeyedWindow(key, new Instant(start), size));
        }

        @Override
        public FixedKeyedWindow getSideInputWindow(final BoundedWindow window) {
            if (window instanceof GlobalWindow) {
                throw new IllegalArgumentException(
                        "Attempted to get side input window for GlobalWindow from non-global WindowFn");
            } else {
                throw new UnsupportedOperationException("Sideinput is not supported in this windowing");
            }
        }

        @Override
        public boolean assignsToSingleWindow() {
            return true;
        }

        @Override
        public Instant getOutputTime(Instant inputTimestamp, FixedKeyedWindow window) {
            return inputTimestamp;
        }

    }



    @EqualsAndHashCode
    public static class FixedKeyedWindow extends BoundedWindow {

        /**
         * Start of the interval, inclusive.
         */
        private final Instant start;

        /**
         * End of the interval, exclusive.
         */
        private final Instant end;

        public String key;

        /**
         * Returns the start of this window, inclusive.
         */
        public Instant start() {
            return start;
        }

        /**
         * Returns the end of this window, exclusive.
         */
        public Instant end() {
            return end;
        }

        public String key() {
            return  key;
        }

        /**
         * Returns the largest timestamp that can be included in this window.
         */
        @Override
        public Instant maxTimestamp() {
            // end not inclusive
            return end.minus(1);
        }

        public FixedKeyedWindow(String key, Instant start, Instant end) {
            this.start = start;
            this.end = end;
            this.key = key;
        }

        public FixedKeyedWindow(String key, Instant start, ReadableDuration size) {
            this(key, start, start.plus(size));
        }


        /**
         * Returns whether this window contains the given window.
         */
        public boolean contains(FixedKeyedWindow other) {
            return this.key.equals(other.key) && !this.start().isAfter(other.start()) && !this.end().isBefore(other.end()) ;
        }

        /**
         * Returns whether this window is disjoint from the given window.
         */
        public boolean isDisjoint(FixedKeyedWindow other) {
            return !this.key.equals(other.key) && !this.end().isAfter(other.start()) || !other.end().isAfter(this.start());
        }

        /**
         * Returns whether this window intersects the given window.
         */
        public boolean intersects(FixedKeyedWindow other) {
            return !isDisjoint(other);
        }


        @Override
        public String toString() {
            return "key: " + key + " [" + start() + ".." + end() + ")";
        }


        /**
         * Returns a {@link Coder} suitable for {@link IntervalWindow}.
         */
        public static Coder<FixedKeyedWindow> getCoder() {
            return FixedKeyedWindowCoder.of();
        }

        /**
         * Encodes an {@link IntervalWindow} as a pair of its upper bound and duration.
         */
        private static class FixedKeyedWindowCoder extends AtomicCoder<FixedKeyedWindow> {

            private static final FixedKeyedWindowCoder INSTANCE = new FixedKeyedWindowCoder();

            private static final Coder<Instant> instantCoder = InstantCoder.of();
            private static final Coder<ReadableDuration> durationCoder = DurationCoder.of();
            private static final Coder<String> keyCoder = StringUtf8Coder.of();

            @JsonCreator
            public static FixedKeyedWindowCoder of() {
                return INSTANCE;
            }

            public void encode(FixedKeyedWindow window, OutputStream outStream, Context context) throws IOException {
                instantCoder.encode(window.end(), outStream, context.nested());
                durationCoder.encode(new Duration(window.start(), window.end()), outStream, context.nested());
                keyCoder.encode(window.key(), outStream, context.nested());
            }


            public FixedKeyedWindow decode(InputStream inStream, Context context) throws IOException {
                Instant end = instantCoder.decode(inStream, context.nested());
                ReadableDuration duration = durationCoder.decode(inStream, context.nested());
                String key = keyCoder.decode(inStream, context.nested());
                return new FixedKeyedWindow(key, end.minus(duration), end);
            }
        }
    }



}
