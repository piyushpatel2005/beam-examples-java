package com.github.piyushpatel2005.udemy.databases;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

public class MongoDBExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> pInput = p.apply(TextIO.read().from("src/main/resources/data/user.csv"));

        PCollection<Document> pDocument = pInput.apply(ParDo.of(new DoFn<String, Document>() {
            @ProcessElement
            public void process(ProcessContext c) {
                String arr[] = c.element().split(",");
                Map<String, Object> mapDoc = new HashMap<>();
                mapDoc.put("userId", arr[0]);
                mapDoc.put("orderId", arr[1]);
                mapDoc.put("Name", arr[2]);
                mapDoc.put("ProductId", arr[3]);
                mapDoc.put("Amount", arr[4]);
                mapDoc.put("OrderDate", arr[5]);
                mapDoc.put("Country", arr[6]);

                Document d1 = new Document(mapDoc);
                c.output(d1);
            }
        }));

        pDocument.apply(MongoDbIO.write()
                .withUri("mongodb://localhost:27017")
                .withDatabase("training")
                .withCollection("users")
        );

        p.run();
    }
}
