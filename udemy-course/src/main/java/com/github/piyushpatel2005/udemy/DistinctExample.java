package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

public class DistinctExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pCustomerList = p.apply(TextIO.read().from("src/main/resources/data/input.csv"));

        pCustomerList.apply(Distinct.<String>create())
                .apply(TextIO.write().to("src/main/resources/data/distinct_input").withNumShards(1).withSuffix(".csv"));

        p.run();
    }
}
