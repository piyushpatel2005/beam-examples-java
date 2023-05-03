package com.github.piyushpatel2005.udemy.options;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyCustomOptions extends PipelineOptions {

    void setInputFile(String filename);
    String getInputFile();

    void setOutputFile(String file);
    String getOutputFile();

    void setFileExtension(String extension);
    String getFileExtension();
}
