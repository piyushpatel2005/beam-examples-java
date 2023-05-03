package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementsExampleWithTypeDescriptors {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> pCustomersList = p.apply(TextIO.read().from("src/main/resources/data/customer.csv"));
        PCollection<String> upperCaseCustomers = pCustomersList.apply(MapElements.into(
                TypeDescriptors.strings()
        ).via(String::toUpperCase));

        upperCaseCustomers.apply(TextIO.write().to("src/main/resources/data/customers").withNumShards(1).withSuffix(".csv"));
        p.run();
    }
}
