package com.github.piyushpatel2005.pluralsight.exploration;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

// Used when we want to make our code more readable
// Composite Transforms extend PTransform and inside this transform, we apply multiple transforms.
// Beam transforms are executed in a distributed manner.
//Multiple copies of the function run on different machines and different processes.class These functions do not communicate or share data.
// These functions are retried on failure. These functions need to be serializable. These functions must be thread compatible.
// Beam SDKs are not thread-safe. Best practice is to make function idempotent.
public class CompositeTransforms19 {
    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("ReadAds", TextIO.read().from("src/main/resources/source/car_ads*.csv"))
                .apply(new MakePriceKVTransform())
                .apply("AveragePrice", Mean.perKey())
                .apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element().getKey() + ": " + c.element().getValue());
                    }
                }));

        pipeline.run().waitUntilFinish();
    }

    public static class MakePriceKVTransform extends PTransform<
            PCollection<String>, PCollection<KV<String, Double>>> {

        @Override
        public PCollection<KV<String, Double>> expand(PCollection<String> lines) {
            return lines.apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                    .apply("MakePriceKVFn", ParDo.of(new MakePriceKVFn()));
        }
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    private static class MakePriceKVFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String make = fields[0];
            Double price = Double.parseDouble(fields[1]);

            c.output(KV.of(make, price));
        }
    }
}
