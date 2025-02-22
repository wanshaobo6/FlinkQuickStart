package org.alliswell.flink._0source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wanshaobo
 * @date 2024/12/17
 */
public class FileSourceOperatorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fs = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("C:\\Users\\wanshaobo\\Desktop\\Alliswell\\EscapePlan\\FlinkQuickStart\\FlinkQuickStart\\src\\main\\java\\org\\alliswell\\flink\\source\\word.txt")).build();
        DataStreamSource<String> ds = env.fromSource(fs, WatermarkStrategy.noWatermarks(), "file");

        ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        ds.print();
        env.execute();
    }
}
