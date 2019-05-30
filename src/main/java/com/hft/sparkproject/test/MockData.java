package com.hft.sparkproject.test;

import com.hft.sparkproject.util.DateUtils;
import com.hft.sparkproject.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * @author : kai.wu
 * @date : 2019/2/9
 */
public class MockData{
    /**
     * 模拟数据
     * @param sc sc
     * @param sqlContext sqlContext
     */
    public static void mock(JavaSparkContext sc,
                            SQLContext sqlContext) {
        List<Row> rows = new ArrayList<>();

        String[] searchKeywords = new String[] {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        String date = DateUtils.getTodayDate();
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        for(int i = 0; i < 100; i++) {
            long userId = random.nextInt(100);

            for(int j = 0; j < 10; j++) {
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);

                Long clickCategoryId = null;

                for(int k = 0; k < random.nextInt(100); k++) {
                    long pageid = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    } else if("click".equals(action)) {
                        if(clickCategoryId == null) {
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        }
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, userId, sessionId,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));
                    rows.add(row);
                }
            }
        }

        createUserVisitAction(sc, sqlContext, rows);
        JavaRDD<Row> rowsRDD;

        /*
         * ==================================================================
         */

        rows.clear();

        createUserInfo(sc, sqlContext, rows, random);

        rows.clear();

        createProductInfo(sc, sqlContext, rows, random);
    }

    private static void createProductInfo(JavaSparkContext sc, SQLContext sqlContext, List<Row> rows, Random random) {
        JavaRDD<Row> rowsRDD;
        int[] productStatus = new int[]{0, 1};

        for(int i = 0; i < 100; i ++) {
            String productName = "product" + i;
            String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";

            Row row = RowFactory.create((long) i, productName, extendInfo);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

        Dataset<Row> df3 = sqlContext.createDataFrame(rowsRDD, schema3);

        df3.toDF().registerTempTable("product_info");
        df3.toDF().show(10);
    }

    private static void createUserInfo(JavaSparkContext sc, SQLContext sqlContext, List<Row> rows, Random random) {
        JavaRDD<Row> rowsRDD;
        String[] sexes = new String[]{"male", "female"};
        for(int i = 0; i < 100; i ++) {
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create((long) i, username, name, age,
                    professional, city, sex);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        Dataset<Row> df2 = sqlContext.createDataFrame(rowsRDD, schema2);

        df2.toDF().registerTempTable("user_info");
        df2.toDF().show(10);
    }

    private static void createUserVisitAction(JavaSparkContext sc, SQLContext sqlContext, List<Row> rows) {
        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("city_id", DataTypes.LongType, true)));

        Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema);


        df.toDF().show(10);
        df.toDF().registerTempTable("user_visit_action");
    }
}
