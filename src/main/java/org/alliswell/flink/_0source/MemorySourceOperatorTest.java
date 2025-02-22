package org.alliswell.flink._0source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author wanshaobo
 * @date 2024/12/17
 */
public class MemorySourceOperatorTest {
    private static final Logger logger = LoggerFactory.getLogger(MemorySourceOperatorTest.class);

    public static void main(String[] args) throws Exception {
        logger.info("Flink job started");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 22, 3));

        ds.print();
        env.execute();
    }
}
