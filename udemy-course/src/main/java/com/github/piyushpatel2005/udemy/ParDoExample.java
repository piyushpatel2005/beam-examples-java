package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class CustomerFilter extends DoFn<String, String> {

    @ProcessElement
    public void processElements(ProcessContext c) {
        String line = c.element();
        String[] arr = line.split(",");
        if (arr[3].equals("Los Angeles")) {
            c.output(line);
        }
    }
}

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pCustomerList = p.apply(TextIO.read().from("src/main/resources/data/customer_pardo.csv"));

        pCustomerList.apply(ParDo.of(new CustomerFilter()))
                .apply(TextIO.write().to("src/main/resources/data/los_angeles").withHeader("id,name,lastname,city").withNumShards(1).withSuffix(".csv"));

        p.run();
    }
}
