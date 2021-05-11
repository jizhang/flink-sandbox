package com.shzhangji.flinksandbox.source;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class UserScores {
  public static void main(String[] args) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    tableEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)\n" +
      "WITH (\n" +
      "  'connector' = 'socket'\n" +
      "  ,'hostname' = 'localhost'\n" +
      "  ,'port' = '9999'\n" +
      "  ,'byte-delimiter' = '10'\n" +
      "  ,'format' = 'changelog-csv'\n" +
      "  ,'changelog-csv.column-delimiter' = '|'\n" +
      ")\n");

    tableEnv.executeSql("SELECT name, SUM(score) FROM UserScores GROUP BY name").print();
  }
}
