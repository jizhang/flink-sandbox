package com.shzhangji.flinksandbox;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class TableJob {
  public static void main(String[] args) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    String path = System.getProperty("user.dir") + "/data/wordcount.txt";
    tableEnv.executeSql("CREATE TABLE wordcount (\n" +
      "  line STRING\n" +
      ")\n" +
      "WITH (\n" +
      "  'connector' = 'filesystem'\n" +
      "  ,'path' = '" + path + "'\n" +
      "  ,'format' = 'csv'\n" +
      ")");

    tableEnv.executeSql("CREATE TABLE wordcount_result (\n" +
      "  word STRING\n" +
      "  ,cnt BIGINT\n" +
      ") WITH ('connector' = 'print')");

    tableEnv.createFunction("TOKENIZER", Tokenizer.class);

    tableEnv.executeSql(
      "INSERT INTO wordcount_result\n" +
      "SELECT word, COUNT(*)\n" +
      "FROM wordcount, LATERAL TABLE(TOKENIZER(line)) AS t(word)\n" +
      "GROUP BY word");
  }

  @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
  public static class Tokenizer extends TableFunction<Row> {
    public void eval(String line) {
      for (String word : line.split("\\s+")) {
        collect(Row.of(word));
      }
    }
  }
}
