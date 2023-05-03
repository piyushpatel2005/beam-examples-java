package com.github.piyushpatel2005.pluralsight.basics;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.util.Arrays;
import java.util.List;

public class Filtering3 {

    public static class FilterThresholdFn extends DoFn<Double, Double> {
        private double threshold = 0;

        public FilterThresholdFn(double threshold) {
            this.threshold = threshold;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element() > threshold) {
                c.output(c.element());
            }
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> stockPrices = Arrays.asList(1367.15, 1360.65, 1401.35, 1414.25, 1389.00, 1390.40, 1411.35);

        pipeline.apply(Create.of(stockPrices))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        System.out.println("Pre-filtering: " + input);
                        return input;
                    }
                }))
                .apply(ParDo.of(new Filtering2.FilterThresholdFn(1400)))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        System.out.println("Post-filtering: " + input);
                        return input;
                    }
                }));

        pipeline.run();
    }
}
