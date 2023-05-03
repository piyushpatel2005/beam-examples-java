package com.github.piyushpatel2005.pluralsight.exploration;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;


// mvn compile exec:java -Dexec.mainClass=com.github.piyushpatel2005.pluralsight.exploration.AveragePriceProcessingWithOptions5 \
// -Dexec.args="--outputFile=src/main/resources/sink/average_price \
// --inputFile=src/main/resources/source/SalesJan2009.csv"

// Run using different execution backend
// mvn compile exec:java -Dexec.mainClass=com.github.piyushpatel2005.pluralsight.exploration.AveragePriceProcessingWithOptions5 \
// -Dexec.args="--outputFile=src/main/resources/sink/average_price \
// --inputFile=src/main/resources/source/SalesJan2009.csv" \
// -Pflink-runner -Dexec.cleanupDaemonThreads=false

// mvn compile exec:java -Dexec.mainClass=com.github.piyushpatel2005.pluralsight.exploration.AveragePriceProcessingWithOptions5 \
// -Dexec.args="--outputFile=src/main/resources/sink/average_price \
// --inputFile=src/main/resources/source/SalesJan2009.csv" \
// -Pspark-runner -Dexec.cleanupDaemonThreads=false
public class AveragePriceProcessingWithOptions5 {
    private static final String CSV_HEADER = "Date,Product,Card,Country";

    public interface AveragePriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/SalesJan2009.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {

        AveragePriceProcessingOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeAveragePriceFn()))
                .apply("AverageAggregation", Mean.perKey())
                .apply("FormatResult", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Double> productCount) ->
                                productCount.getKey() + "," + productCount.getValue()))
                .apply("WriteResult",
                        TextIO.write()
                                .to(options.getOutputFile())
                                .withSuffix(".csv")
                                .withShardNameTemplate("-SSS")
                                .withHeader("Product,AveragePrice"));

        pipeline.run().waitUntilFinish();

        System.out.println("******Current runner: " + pipeline.getOptions().getRunner());
        System.out.println("Pipeline execution complete!");
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

    private static class ComputeAveragePriceFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            String[] data = c.element().split(",");

            String product = data[1];
            Double price = Double.parseDouble(data[2]);

            c.output(KV.of(product, price));
        }
    }
}
