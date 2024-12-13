package org.alliswell.flink.api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * @author wanshaobo
 * @date 2024/11/27
 */
public class FlinkApi {
    public static void main(String[] args) {
        Map<String, String> map1 = new HashMap<>();
        Map<String, String> map2 = new HashMap<>();

        map1.put("生产", "1");
        map1.put("同城", "2");

        map2.put("生产", "1");
        map2.put("同城", "111");

        // 判断两个集合是否相等
        if (map1.keySet().equals(map2.keySet())) {
            System.out.println("两个集合的元素完全相同！");
            System.out.println("测试区别");
        } else {
            System.out.println("两个集合的元素不同！");
        }
    }

}
