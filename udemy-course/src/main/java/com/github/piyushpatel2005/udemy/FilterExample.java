package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;



// Filter Los angeles customers
class MyFilter implements SerializableFunction<String, Boolean> {
    @Override
    public Boolean apply(String input) {
        return input.contains("Los Angeles");
    }
}

public class FilterExample {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> pCustomerList = p.apply(TextIO.read().from("src/main/resources/data/customer_pardo.csv"));
        PCollection<String> pOutput = pCustomerList.apply(Filter.by(new MyFilter()));

        pOutput.apply(TextIO.write().to("src/main/resources/data/los_angeles").withHeader("id,name,lastname,city").withNumShards(1).withSuffix(".csv"));


        p.run().waitUntilFinish();
    }
}
