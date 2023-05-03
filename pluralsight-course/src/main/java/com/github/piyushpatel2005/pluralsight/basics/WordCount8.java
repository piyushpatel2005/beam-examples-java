package com.github.piyushpatel2005.pluralsight.basics;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WordCount8 {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/source/words.txt"))
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.toLowerCase().split(" "))))

                .apply("CountWords", Count.<String>perElement())

                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) ->
                                wordCount.getKey() + ": " + wordCount.getValue()))

                .apply(TextIO.write().to("src/main/resources/sink/word_count"));

        pipeline.run().waitUntilFinish();
    }
}
