package org.alliswell.flink._05processfunc;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author wanshaobo
 * @date 2024/12/24
 */
public class ProcessFunTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> text = env.socketTextStream("localhost", 7777);
        text.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                System.out.printf("Value: %s%n", s);
                if(s.length() % 2== 0) {
                    collector.collect(s);
                }
            }
        }).print();

        env.execute();
    }
}
