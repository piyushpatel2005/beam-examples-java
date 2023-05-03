# Apache Beam Java

Beam's pipelines are portable between multiple runners. It is also portable between various programming languages. The logic of data processing is also portable between bounded and unbounded datasets.

To start using Apache Beam, we only need Beam SDK dependencies.

```xml
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-core</artifactId>
    <version>2.19.0</version>
</dependency>

<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-runners-direct-java</artifactId>
    <version>2.19.0</version>
</dependency>
```

The fundamental datatype is `PCollection`. It has following characteristics.
- Element type should be the same
- It is immutable
- Randome access won't be allowed
- It can be either bounded or unbounded
- Each element has timestamp associated with it.

Following are the main options to run Beam pipeline

1. Create a `Pipeline` object
2. Create PCollection and apply different transformations
3. Write to a sink
4. Run pipeline.

`PipelineOptions` provide a mechanism to pass different configuration for production usecases. For example, in our case `com.github.piyushpatel2005.udemy.options.MyCustomOptions` defines custom options which can be specified while executing this pipeline through command line.
In order to do this, we have to extend `PipelineOptions` interface and specify signature for getters and setters for our options.
This `MyCustomOptions` interface is used in `ReadingLocalFiles` to read options.

`PTransform` represents a data processing operation or a step. It converts `PCollection` into another `PCollection`. The output of `PTransform` is another `PCollection`.
There are few built-in `PTransform`s and these are applied using `apply` method on `PCollection`.

- `MapElements` is element wise transformation. There are two approaches to implement transformations, i.e. using TypeDescriptors or using function.
- `ParDo` is element-wise transform, invoking a user defined function on each of the elements of the input `PCollection`. With the help of `DoFn`, we can implement `ParDo` transform.
- `Flatten` can be used to read inputs from multiple files and union them
- `PartitionFn` can be used to define custom partitioner for your input data.
- `Distinct<T>` takes a `PCollection<T>` and returns a `PCollection<T>` that has all distinct elements of the input.
- `Count` is used to count records.
- `GroupByKey` is used to group values associated with a key.

apache Beam also allows for creating custom IO connectors. All Beam sources and sinks are composite transforms. These are series of simple transforms performed in parallel.

