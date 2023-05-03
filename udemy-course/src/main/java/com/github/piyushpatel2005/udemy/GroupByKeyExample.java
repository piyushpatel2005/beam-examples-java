package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


class StringToKV extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void process(ProcessContext c) {
        String input = c.element();
        String arr[] = input.split(",");
        c.output(KV.of(arr[0], Integer.valueOf(arr[3])));
    }
}


class KVToString extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void process(ProcessContext c) {
        String customerId = c.element().getKey();
        Iterable<Integer> prices = c.element().getValue();
        Integer sum = 0;
        for (Integer price: prices) {
            sum = sum + price;
        }
        c.output(customerId + "," + sum.toString());
    }
}

public class GroupByKeyExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // Read input data
        // contains customerId,orderId,productId,price
        PCollection<String> pCustomerOrderList = p.apply(TextIO.read().from("src/main/resources/data/customer_orders.csv"));

        // Convert to KV pairs
        PCollection<KV<String, Integer>> pCustomerPriceKV = pCustomerOrderList.apply(ParDo.of(new StringToKV()));

        // Apply groupbyKey and build KV<String, Iterable<Long>>
        PCollection<KV<String, Iterable<Integer>>> pGroupedCustomerPrice = pCustomerPriceKV.apply(GroupByKey.<String, Integer>create());

        // Convert KV<String, Iterable<Long>> to String
        PCollection<String> totalPricePerCustomer = pGroupedCustomerPrice.apply(ParDo.of(new KVToString()));

        totalPricePerCustomer.apply(TextIO.write().to("src/main/resources/data/customer_orders").withNumShards(1).withSuffix(".csv"));
        p.run();
    }
}
