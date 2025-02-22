package org.alliswell.flink._1transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author wanshaobo
 * @date 2024/12/18
 */
public class RepartitionTest {
    //把流中的数据随机打乱，均匀地传递到下游任务分区

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5)).setParallelism(1);

        // 将数据打乱随机均匀分发给下游, 每次执行的结果可能不同
//        ds.shuffle().print();

        // 轮询，简单来说就是“发牌”，按照先后顺序将数据做依次分发。通过调用DataStream的.rebalance()方法，就可以实现轮询重分区。
//        ds.rebalance().print();

        //重缩放分区和轮询分区非常相似。当调用rescale()方法时，其实底层也是使用Round-Robin算法进行轮询，但是只会将数据==轮询发送到下游并行任务的一部分中==。rescale的做法是分成小团体，发牌人==只给自己团体内的所有人轮流发牌==。
//        ds.rescale().print();

        //全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，会将所有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行度变成了1，所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。
//        ds.global().print();

//        当Flink提供的所有分区策略都不能满足用户的需求时，我们可以通过使用partitionCustom()方法来自定义分区策略。
//        ds.partitionCustom().print();

        env.execute();
    }
}
