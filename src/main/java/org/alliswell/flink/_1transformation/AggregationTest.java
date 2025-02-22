package org.alliswell.flink._1transformation;

import org.alliswell.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author wanshaobo
 * @date 2024/12/18
 */
public class AggregationTest {
    public static void main(String[] args) throws Exception {
    //        DataStream是没有直接进行聚合的API的。因为我们对海量数据做聚合肯定要进行分区并行处理，这样才能提高效率。所以在Flink中，要做聚合，需要先进行分区；这个操作就是通过keyBy来完成的。

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream = env.fromElements(new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3),
                new WaterSensor("sensor_3", 4L, 1),
                new WaterSensor("sensor_3", 4L, 100));

        stream.keyBy(item -> item.id).max("vc").print();

        env.execute();
    }
}
