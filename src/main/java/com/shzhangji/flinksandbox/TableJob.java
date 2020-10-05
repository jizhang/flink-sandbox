package com.shzhangji.flinksandbox;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class TableJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    TableSource source = new CsvTableSource.Builder()
      .path("data/wordcount.txt")
      .field("line", Types.STRING)
      .build();
    tableEnv.registerTableSource("wordcount", source);
    tableEnv.registerFunction("TOKENIZER", new Tokenizer());
    Table wordcountTable = tableEnv.sqlQuery(
      "SELECT word, COUNT(*) AS cnt" +
        " FROM wordcount, LATERAL TABLE(TOKENIZER(line)) AS t (word)" +
        " GROUP BY word");
    DataStream<?> stream = tableEnv.toRetractStream(wordcountTable, Row.class);
    stream.print();

    env.execute();
  }

  public static class Tokenizer extends TableFunction<Row> {
    public void eval(String line) {
      for (String word : line.split("\\s+")) {
        collect(Row.of(word));
      }
    }
  }
}
