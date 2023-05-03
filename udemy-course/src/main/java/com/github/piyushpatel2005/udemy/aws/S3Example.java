package com.github.piyushpatel2005.udemy.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class S3Example {
    // provide command line arguments as below.
    // --AWSAccessKey=<AWS_ACCESS_KEY> --AWSSecretKey=<AWS_SECRET_KEY> --awsRegion=<AWS_REGION>
    public static void main(String[] args) {
        AWSS3Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AWSS3Options.class);

        Pipeline pipeline = Pipeline.create(options);

        AWSCredentials awsCredentials = new BasicAWSCredentials(options.getAWSAccessKey(), options.getAWSSecretKey());
        options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredentials));

        PCollection<String> pInput = pipeline.apply(TextIO.read().from("s3://bucket/folder/user_order.csv"));
        pInput.apply(ParDo.of(new DoFn<String, Void>() {

            @ProcessElement
            public void process(ProcessContext c) {
                System.out.println(c.element());
            }
        }));
    }
}
