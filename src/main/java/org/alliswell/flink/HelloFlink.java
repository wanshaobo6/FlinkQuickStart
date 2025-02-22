package org.alliswell.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wanshaobo
 * @date 2025/2/22
 */
public class HelloFlink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("172.29.81.139", 7777);
        source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            String[] splitStr = s.split(" ");
            for (String c : splitStr) {
                collector.collect(Tuple2.of(c, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(t -> t.f0)
          .sum(1).print();
        env.execute();
    }
}
