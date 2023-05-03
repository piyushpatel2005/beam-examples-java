package com.github.piyushpatel2005.udemy;

import com.github.piyushpatel2005.udemy.options.MyCustomOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class ReadingLocalFiles {
    public static void main(String[] args) {
        MyCustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyCustomOptions.class);
//  To execute this code with command line, we can build the jar file and invoke with below arguments.
//  java -cp beam-java-0.0.1-jar-with-dependencies.jar com.github.piyushpatel2005.udemy.ReadingLocalFiles --inputFile="src\\main\\resources\\data\\input.csv" --outputFile="src\\main\\resources\\data\output" --fileExtension=".csv"
        Pipeline pipeline = Pipeline.create(options);
//        PCollection<String> output = pipeline.apply(TextIO.read().from("src/main/resources/data/input.csv"));
        // using PipelineOptions we can remove hardcoded values from below lines.
        PCollection<String> output = pipeline.apply(TextIO.read().from(options.getInputFile()));
//        output.apply(TextIO.write().to("src/main/resources/data/output.csv").withNumShards(1).withSuffix(".csv"));
        output.apply(TextIO.write().to(options.getOutputFile()).withNumShards(1).withSuffix(options.getFileExtension()));
        pipeline.run();
    }
}
