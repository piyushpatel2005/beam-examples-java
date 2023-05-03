package com.github.piyushpatel2005.pluralsight.basics;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.util.Arrays;
import java.util.List;

public class Aggregation4 {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> stockPrices = Arrays.asList(1367.15, 1360.65, 1401.35, 1414.25, 1389.00, 1390.40, 1411.35);

        pipeline.apply(Create.of(stockPrices))
                .apply(Mean.<Double>globally())
                .apply(MapElements.via(new SimpleFunction<Double, Void>() {
                    @Override
                    public Void apply(Double input) {
                        System.out.println("Average stock price: " + input);
                        return null;
                    }
                }));

        pipeline.run();
    }
}
