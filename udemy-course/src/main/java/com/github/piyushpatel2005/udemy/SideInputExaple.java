package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class SideInputExaple {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        // read customers who have returned a product.
        PCollection<KV<String, String>> pReturnCustomers = p.apply(TextIO.read().from("src/main/resources/data/return.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    // Create key value pair from return.csv for customerID and CustomerName
                    @ProcessElement
                    public void process(ProcessContext c) {
                        String[] arr = c.element().split(",");
                        c.output(KV.of(arr[0], arr[1]));
                    }
                }));

        PCollectionView<Map<String, String>> pMap = pReturnCustomers.apply(View.asMap());

        PCollection<String> pCustomerList = p.apply(TextIO.read().from("src/main/resources/data/cust_order.csv"));

        pCustomerList.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext c) {
                Map<String, String> pSideInputView = c.sideInput(pMap);
                String[] arr = c.element().split(",");
                String customerName = pSideInputView.get(arr[0]);
//                System.out.println(customerName);
                if (customerName == null) {
                    // print only customers who never returned a product.
                    System.out.println(c.element());
                }
            }
        }).withSideInputs(pMap));

        p.run();

    }
}
