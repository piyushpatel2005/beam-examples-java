package com.github.piyushpatel2005.udemy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String, String> {
    @Override
    public String apply(String input) {
        String arr[] = input.split(",");
        String sessionId = arr[0];
        String userId = arr[1];
        String username = arr[2];
        String videoId = arr[3];
        String duration = arr[4];
        String startTime = arr[5];
        String sex = arr[6];

        String output = "";
        if (sex.equals("1")) {
            output = sessionId + "," + userId + "," + username + "," + videoId + "," + duration + "," + startTime + "," + "male";
        } else if (sex.equals("2")){
            output = sessionId + "," + userId + "," + username + "," + videoId + "," + duration + "," + startTime + "," + "female";
        } else {
            output = input;
        }

        return output;
    }
}

public class MapElementsExampleWithFunction {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> pUserList = p.apply(TextIO.read().from("src/main/resources/data/user.csv"));

        // convert sex column to male and female
        PCollection<String> transformedUsers = pUserList.apply(MapElements.via(new User()));

        transformedUsers.apply(TextIO.write().to("src/main/resources/data/user_output").withNumShards(1).withSuffix(".csv"));
        p.run();
    }
}
