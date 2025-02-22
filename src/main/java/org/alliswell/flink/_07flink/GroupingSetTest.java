package org.alliswell.flink._07flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author wanshaobo
 * @date 2025/2/14
 */
public class GroupingSetTest {
    public static void main(String[] args) {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment env = TableEnvironment.create(environmentSettings);

        Table tb = env.sqlQuery("SELECT\n" +
                "  supplier_id\n" +
                ", rating\n" +
                ", product_id\n" +
                ", COUNT(*)\n" +
                "FROM (\n" +
                "VALUES\n" +
                "  ('supplier1', 'product1', 4),\n" +
                "  ('supplier1', 'product2', 3),\n" +
                "  ('supplier2', 'product3', 3),\n" +
                "  ('supplier2', 'product4', 4)\n" +
                ")\n" +
                "-- 供应商id、产品id、评级\n" +
                "AS Products(supplier_id, product_id, rating)  \n" +
                "GROUP BY GROUPING SETS(\n" +
                "  (supplier_id, product_id, rating),\n" +
                "  (supplier_id, product_id),\n" +
                "  (supplier_id, rating),\n" +
                "  (supplier_id),\n" +
                "  (product_id, rating),\n" +
                "  (product_id),\n" +
                "  (rating),\n" +
                "  ()\n" +
                ");");
        tb.execute().print();
    }
}
