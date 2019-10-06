package com.shzhangji.flinksandbox.kafka;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaLoader {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // checkpoint
    env.enableCheckpointing(10_000);
    env.setStateBackend((StateBackend) new FsStateBackend("file:///tmp/flink/checkpoints"));
    CheckpointConfig config = env.getCheckpointConfig();
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

    // source
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
        "flink_test", new SimpleStringSchema(), props);
    DataStream<String> stream = env.addSource(consumer);

    // sink
    RollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.create()
        .withRolloverInterval(15_000)
        .build();
    StreamingFileSink<String> sink = StreamingFileSink
        .forRowFormat(new Path("file:///tmp/kafka-loader"), new SimpleStringEncoder<String>())
        .withBucketAssigner(new EventTimeBucketAssigner())
        .withRollingPolicy(rollingPolicy)
        .withBucketCheckInterval(1000)
        .build();

    stream.addSink(sink);

    env.execute();
  }
}
