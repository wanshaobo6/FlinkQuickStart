package org.alliswell.flink._07flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author wanshaobo
 * @date 2025/2/14
 */
public class FlinkSql {
    public static void main(String[] args) {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment env = TableEnvironment.create(environmentSettings);
        String sourceTable = "CREATE TABLE source1 (\n" +
                "  dim STRING,\n" +
                "  user_id BIGINT,\n" +
                "  price BIGINT,\n" +
                "  row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1000',\n" +
                "  'fields.dim.length' = '1',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '10000000000',\n" +
                "  'fields.price.min' = '1',\n" +
                "  'fields.price.max' = '100000'\n" +
                ");\n";
       String sinkTable =  "CREATE TABLE sink1 (\n" +
                "  dim STRING,\n" +
                "  pv BIGINT,\n" +
                "  sum_price BIGINT,\n" +
                "  max_price BIGINT,\n" +
                "  min_price BIGINT,\n" +
                "  uv BIGINT,\n" +
                "  window_start bigint\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");";
        env.executeSql(sourceTable);
        env.executeSql(sinkTable);

        Table tb = env.sqlQuery("select * from source1;");
        tb.execute().print();
    }
}
