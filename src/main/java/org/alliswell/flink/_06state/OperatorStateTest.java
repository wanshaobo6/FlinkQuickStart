package org.alliswell.flink._06state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wanshaobo
 * @date 2024/12/31
 */
public class OperatorStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        SingleOutputStreamOperator<Long> ds = env.socketTextStream("localhost", 7777).map(new MapCountFunction());

        ds.print();

        env.execute();
    }

    public static class MapCountFunction extends RichMapFunction<String, Long> {
        private ListState<Long> state;
        
        @Override
        public Long map(String s) throws Exception {
            return null;
        }
    }
}
