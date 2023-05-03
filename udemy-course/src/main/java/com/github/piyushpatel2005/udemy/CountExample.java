package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> pCustomerList = p.apply(TextIO.read().from("src/main/resources/data/cust_order.csv"));

        PCollection<Long> count = pCustomerList.apply(Count.globally());
        count.apply(ParDo.of(new DoFn<Long, Void>() {
            @ProcessElement
            public void process(ProcessContext c) {
                System.out.println(c.element());
            }
        }));
        p.run();
    }
}
