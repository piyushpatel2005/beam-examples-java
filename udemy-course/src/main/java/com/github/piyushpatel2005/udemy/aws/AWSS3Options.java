package com.github.piyushpatel2005.udemy.aws;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;

public interface AWSS3Options extends PipelineOptions, S3Options {
    void setAWSAccessKey(String value);
    String getAWSAccessKey();

    void setAWSSecretKey(String value);
    String getAWSSecretKey();

    void setAWSRegion(String value);
    String getAWSRegion();
}
