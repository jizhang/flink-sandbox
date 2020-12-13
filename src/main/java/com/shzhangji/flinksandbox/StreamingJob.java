package com.shzhangji.flinksandbox;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> ds = env.readTextFile("data/wordcount.txt");
    ds.flatMap(new Tokenizer()).keyBy(t -> t.f0).sum(1).print();

    env.execute("Flink Streaming Java API Skeleton");
  }

  public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
      for (String word : value.split("\\s+")) {
        out.collect(Tuple2.of(word, 1));
      }
    }
  }
}
