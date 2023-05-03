package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class RightJoinExample {

    private static class OrderParsing extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void process(ProcessContext c) {
            String arr[] = c.element().split(",");
            String strKey = arr[0];
            String strVal = arr[1] + "," + arr[2] + "," + arr[3];
            c.output(KV.of(strKey, strVal));
        }
    }

    private static class UserParsing extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void process(ProcessContext c) {
            String arr[] = c.element().split(",");
            String strKey = arr[0];
            String strVal = arr[1];
            c.output(KV.of(strKey, strVal));
        }
    }

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<KV<String, String>> users = p.apply(TextIO.read().from("src/main/resources/data/users.csv"))
                .apply(ParDo.of(new UserParsing()));
        PCollection<KV<String, String>> userOrders = p.apply(TextIO.read().from("src/main/resources/data/users_orders.csv"))
                .apply(ParDo.of(new OrderParsing()));
        // create TupleTag
        final TupleTag<String> orderTuple = new TupleTag<>();
        final TupleTag<String> userTuple = new TupleTag<>();
        // Combine the datasets using CoGroupByKey
        PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(orderTuple, userOrders)
                .and(userTuple, users)
                .apply(CoGroupByKey.<String> create());

        // Iterate CoGbkResult and build String
        PCollection<String> output = result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void process(ProcessContext c ) {
                String strKey = c.element().getKey();
                CoGbkResult valObject = c.element().getValue();
                Iterable<String> orderTable = valObject.getAll(orderTuple);
                Iterable<String> usersTable = valObject.getAll(userTuple);

                for (String user: usersTable) {
                    if (orderTable.iterator().hasNext()) {
                        for (String order: orderTable) {
                            c.output(strKey + "," + user + "," + order);
                        }
                    } else {
                        c.output(strKey + "," + user + ",null,null,null");
                    }
                }

                for (String order: orderTable) {
                    if (usersTable.iterator().hasNext()) {
                        for (String user : usersTable) {
                            c.output(strKey + "," + order + "," + user);
                        }
                    } else {
                        c.output(strKey + "," + order + "," + null);
                    }
                }
            }
        }));

        output.apply(TextIO.write().to("src/main/resources/data/join").withNumShards(1).withSuffix(".csv"));

        p.run();
    }
}
