package com.github.piyushpatel2005.udemy;

import com.github.piyushpatel2005.udemy.domain.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<CustomerEntity> pList = pipeline.apply(Create.of(getCustomers()));

        // Below line gives error because TextIO.write supports writing only PCollection<String>
        // So we need a way to convert Java objects into PCollection
//        pList.apply(TextIO.write().to("src/main/resources/data/customers"))

        PCollection<String> pStrList = pList.apply(MapElements.into(TypeDescriptors.strings()).via(CustomerEntity::getName));
        pStrList.apply(TextIO.write().to("src/main/resources/data/customers").withNumShards(1).withSuffix("csv"));

        pipeline.run();
    }

    static List<CustomerEntity> getCustomers() {
        CustomerEntity customer1 = new CustomerEntity("100", "John");
        CustomerEntity customer2 = new CustomerEntity("101", "Kevin");

        List<CustomerEntity> customers = new ArrayList<>();
        customers.add(customer1);
        customers.add(customer2);

        return customers;
    }
}
