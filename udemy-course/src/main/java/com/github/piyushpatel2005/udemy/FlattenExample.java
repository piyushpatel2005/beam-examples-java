package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> pCustomerList1 = p.apply(TextIO.read().from("src/main/resources/data/customer_1.csv"));
        PCollection<String> pCustomerList2 = p.apply(TextIO.read().from("src/main/resources/data/customer_2.csv"));
        PCollection<String> pCustomerList3 = p.apply(TextIO.read().from("src/main/resources/data/customer_3.csv"));

        PCollectionList<String> list = PCollectionList.of(pCustomerList1).and(pCustomerList2).and(pCustomerList3);
        PCollection<String> pAllCustomerList = list.apply(Flatten.pCollections());
        pAllCustomerList.apply(TextIO.write().to("src/main/resources/data/all_customer").withHeader("id,name,lastname,city").withNumShards(1).withSuffix(".csv"));


        p.run().waitUntilFinish();
    }
}
