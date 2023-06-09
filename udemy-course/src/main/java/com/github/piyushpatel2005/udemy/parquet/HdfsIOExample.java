package com.github.piyushpatel2005.udemy.parquet;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;

import java.util.Collections;

public class HdfsIOExample {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://namenode:8020");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        String[] args1 = new String[]{"--hdfsConfiguration=[{\"fs.default.name\": \"hdfs://namenode:8020\"}]",
        "--runner=DirectRunner"};

        HadoopFileSystemOptions hdfsOptions = PipelineOptionsFactory.fromArgs(args1)
                .withValidation()
                .as(HadoopFileSystemOptions.class);

        hdfsOptions.setHdfsConfiguration(Collections.singletonList(conf));

        Pipeline p = Pipeline.create(hdfsOptions);

        PCollection<String> pUsers = p.apply(TextIO.read().from("hdfs://namenode:8020/user/user.csv"));
        pUsers.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        p.run();
    }
    // java -cp <jar_file_name> <fully_qualified_class>
}
