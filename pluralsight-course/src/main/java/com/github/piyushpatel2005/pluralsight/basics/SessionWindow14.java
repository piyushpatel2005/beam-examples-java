package com.github.piyushpatel2005.pluralsight.basics;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class SessionWindow14 {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> carMakesTimes = pipeline.apply(Create.timestamped(
                TimestampedValue.of("Ford",   new DateTime("2020-12-12T20:30:05").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:06").toInstant()),
                TimestampedValue.of("Ford",   new DateTime("2020-12-12T20:30:07").toInstant()),
                TimestampedValue.of("Ford",   new DateTime("2020-12-12T20:30:08").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:11").toInstant()),
                TimestampedValue.of("Ford",   new DateTime("2020-12-12T20:30:12").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:13").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:14").toInstant()),
                TimestampedValue.of("Ford",   new DateTime("2020-12-12T20:30:16").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:17").toInstant())));

        PCollection<String> windowedMakesTimes = carMakesTimes.apply(
                "Window", Window.into(Sessions.withGapDuration(Duration.standardSeconds(3))));

        PCollection<KV<String, Long>> output = windowedMakesTimes.apply(Count.perElement());

        output.apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {

            @ProcessElement
            public void processElement(ProcessContext c, BoundedWindow window)  {

                System.out.println(String.format(
                        "%s: %s %s", window.maxTimestamp(), c.element().getKey(), c.element().getValue()));
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
