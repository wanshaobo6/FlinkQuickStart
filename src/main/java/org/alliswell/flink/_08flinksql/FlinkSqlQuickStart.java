package org.alliswell.flink._08flinksql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wanshaobo
 * @date 2025/2/22
 */
public class FlinkSqlQuickStart {
    public static void main(String[] args) throws Exception {
        // Lesson1:创建执行环境
        // 方式1
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 方式2
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Lesson2: 创建表
        // 方式1  调用executeSql 然后参数是DDL语句
        // 方式2  调用sqlQuery方法后得到的是一个Table对象, 如果想要在后续的Sql中用到这个临时表, 则需要将该临时表注册到环境中 createTemporaryView
        String sourceTable = "CREATE TABLE source (\n" +
                "  user_id BIGINT,\n" +
                "  price BIGINT,\n" +
                "  row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '100',\n" +
                "  'fields.price.min' = '1',\n" +
                "  'fields.price.max' = '100'\n" +
                ");\n";
        tableEnv.executeSql(sourceTable);
        Table table = tableEnv.sqlQuery("select * from source where price > 50");
        tableEnv.createTemporaryView("luxury", table);

        // Lesson3: 查询表
        // 方式1: 执行Sql查询 executeSql
        // 方式2: 调用TableAPI查询
        Table result = tableEnv.from("source").where($("price").isGreaterOrEqual(90))
                .select($("price"), $("row_time"));

        // Lesson4: 输出表
        tableEnv.executeSql("create table tb_slink (\n" +
                "  price BIGINT,\n" +
                "  row_time TIMESTAMP(3)\n" +
                ") with (\n" +
                "  'connector'='print'\n" +
                ")");
//        result.executeInsert("tb_slink");

        // Lesson5: 表和流的转换
        // 将流转成表 方法1:fromDataStream
        tableEnv.executeSql("create table word (\n" +
                "  word STRING,\n" +
                "  cnt BIGINT\n" +
                ") with (\n" +
                "  'connector'='print'\n" +
                ")");
        DataStreamSource<String> ds = env.fromElements("Hello", "Flink", "Hello", "Love And Peace");
//        Table dsTable = tableEnv.fromDataStream(ds, $("f0"));
//        dsTable.groupBy($("f0")).select($("f0").as("word"), $("f0").count().as("cnt")).executeInsert("word");
        // 将流转成表, 并且将该表添加到环境中 方法2:createTemporaryView
        // 将表转成流 tableEnv.toDataStream(追加流) tableEnv.toChangelogStream 追加流
//        DataStream<Row> dataStream = tableEnv.toDataStream(dsTable);
//        DataStream<Row> changelogStream = tableEnv.toChangelogStream(dsTable);


        // Lesson5: UDF
        // ScalarFunction
        // TableFunction
        // AggregateFunction
        // TableAggregateFunction
        tableEnv.createTemporaryFunction("split_words", ExplodeFunction.class);
        tableEnv.createTemporaryView("str", ds, $("words"));
        Table r1 = tableEnv.sqlQuery("select words, word, length from str left join lateral table(split_words(words)) on true");
        tableEnv.toDataStream(r1).print();

        env.execute();
    }

    @FunctionHint(output = @DataTypeHint("Row<word String, length INT>"))
    public static class ExplodeFunction extends TableFunction<Row> {
        // 返回是 void，用 collect方法输出
        public void eval(String str) {
            for (String word : str.split(" ")) {
                collect(Row.of(word, word.length()));
            }
        }
    }
}
