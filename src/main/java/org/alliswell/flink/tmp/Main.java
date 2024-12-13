package org.alliswell.flink.tmp;

import java.util.*;
import java.util.stream.*;

public class Main {
    public static void main(String[] args) {
        List<String[]> list = new ArrayList<>();
        
        // 添加一些示例数据
        list.add(new String[]{"a", "b"});
        list.add(new String[]{"b", "a"});
        list.add(new String[]{"a", "b"});  // 重复元素
        list.add(new String[]{"c", "d"});

        // 使用 Stream 和 Set 去重
        Set<String[]> uniqueList = list.stream()
                .collect(Collectors.toSet());
        uniqueList.add(new String[]{"a", "b"});
        uniqueList.add(new String[]{"a11", "b"});

        // 输出去重后的列表
        System.out.println("去重后的数组列表：");
        for (String[] array : uniqueList) {
            System.out.println(Arrays.toString(array));
        }
    }
}