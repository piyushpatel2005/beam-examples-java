package com.github.piyushpatel2005.pluralsight.basics;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreWithPipelineOptions7 {
    private static final String CSV_HEADER = "ID,Name,Physics,Chemistry,Math,English,Biology,History";

    public interface TotalScoreComputationOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/student_scores.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path to write the output to")
        @Validation.Required
        @Default.String("src/main/resources/sink/student_total")
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {
        TotalScoreComputationOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(TotalScoreComputationOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoresFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(TextIO.write()
                        .withNumShards(1)
                        .withSuffix(".csv")
                        .to(options.getOutputFile()));

        pipeline.run().waitUntilFinish();

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

    private static class ComputeTotalScoresFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void process(ProcessContext c) {
            String[] data = c.element().split(",");
            String name = data[1];
            Integer totalScore = Integer.parseInt(data[2]) + Integer.parseInt(data[3]) +
                    Integer.parseInt(data[4]) + Integer.parseInt(data[5]) +
                    Integer.parseInt(data[6]) + Integer.parseInt(data[7]);
            c.output(KV.of(name, totalScore));
        }
    }

    private static class ConvertToStringFn extends DoFn<KV<String, Integer>, String> {
        @ProcessElement
        public void process(ProcessContext c) {
            c.output(c.element().getKey() + "," + c.element().getValue());
        }
    }
}
