package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * Partition data based on city.
 * 0 -> Los Angeles
 * 1 -> Phoenix
 * 2 -> New York
 */
class MyCityPartitioner implements Partition.PartitionFn<String> {

    @Override
    public int partitionFor(String elem, int numPartitions) {
        String[] arr = elem.split(",");
        String city = arr[3];
        if (city.equals("Los Angeles")) {
            return 0;
        } else if (city.equals("Phoenix")) {
            return 1;
        } else {
            return 2;
        }
    }
}

public class PartitionExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pCustomerList = p.apply(TextIO.read().from("src/main/resources/data/partition.csv"));
        PCollectionList<String> partitionedCustomerList = pCustomerList.apply(Partition.of(3, new MyCityPartitioner()));

        PCollection<String> losAngelesCustomers = partitionedCustomerList.get(0);
        PCollection<String> phoenixCustomers = partitionedCustomerList.get(1);
        PCollection<String> newYorkCustomers = partitionedCustomerList.get(2);

        losAngelesCustomers.apply(TextIO.write().to("src/main/resources/data/losangeles").withNumShards(1).withSuffix(".csv"));
        phoenixCustomers.apply(TextIO.write().to("src/main/resources/data/phoenix").withNumShards(1).withSuffix(".csv"));
        newYorkCustomers.apply(TextIO.write().to("src/main/resources/data/newyork").withNumShards(1).withSuffix(".csv"));

        p.run();
    }
}
