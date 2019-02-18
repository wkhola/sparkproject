package com.hft.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.hft.sparkproject.conf.ConfigurationManager;
import com.hft.sparkproject.constant.Constants;
import com.hft.sparkproject.dao.ITaskDAO;
import com.hft.sparkproject.dao.factory.DAOFactory;
import com.hft.sparkproject.domain.Task;
import com.hft.sparkproject.test.MockData;
import com.hft.sparkproject.util.ParamUtils;
import com.hft.sparkproject.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;
import scala.tools.cmd.gen.AnyVals;

import java.util.Iterator;

/**
 * 用户访问session分析spark作业
 * @author : kai.wu
 * @date : 2019/2/9
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        // 构建上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContest(sc.sc());

        // 生成模拟数据
        mockData(sc, sqlContext);

        //获取DAO辅助组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        //如果进行session粒度的数据聚合
        // 首先从user_visit_action表中，过滤出时间
        Long taskid = ParamUtils.getTaskIdFromArgs(args);

        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> acionRDD = getActionRDDByDateRange(sqlContext, taskParam);


        // 关闭spark上下文
        sc.close();
    }

    /**
     * 获取SQLContext
     * 如果是本地模式的话就是SQLContext
     * 如果是生产环境运行的话就是HiveContext
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContest(SparkContext sc){
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     *  生成模拟数据（只有本地模式， 才会去生成模拟数据）
     * @param sc sc
     * @param sqlContext SQLContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext){
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     *  获取指定日期范围内的用户访问行为数据
     * @param sqlContext SQLContext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * " +
                     "from user_visit_action " +
                     "where date >= '" + startDate + "' " +
                     "and date <= '" + endDate + "'";
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 按照对行为数据session粒度聚合
     * @param actionRDD 行为数据RDD
     * @return session行为聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext){
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
                (PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2), row));

        // 对session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> userid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = userid2ActionsRDD.mapToPair(
                (PairFunction<Tuple2<String, Iterable<Row>>, Long, String>) tuple2 -> {

            String sessionid = tuple2._1;
            Iterator<Row> iterator = tuple2._2.iterator();

            StringBuffer searchKeywordsBuffer = new StringBuffer("");
            StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
            Long userid = null;
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (userid == null) {
                    userid = row.getLong(1);
                }
                String searchKeyword = row.getString(5);
                Long clickCategoryId = row.getLong(6);

                if (StringUtils.isNotEmpty(searchKeyword)) {
                    if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                        searchKeywordsBuffer.append(searchKeyword).append(",");
                    }
                }
                if (StringUtils.isNotEmpty(String.valueOf(clickCategoryId))) {
                    if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                        clickCategoryIdsBuffer.append(clickCategoryId).append(",");
                    }
                }
            }
            String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
            String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
            String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                    + Constants.FIELD_SEARCH_KEY_WORDS + "=" + searchKeywords + "|"
                    + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
            return new Tuple2<>(userid, partAggrInfo);
        });

        // 查询所有用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
                (PairFunction<Row, Long, Row>) row -> {
            Long userid = row.getLong(0);
            return new Tuple2<>(userid, row);
        });

        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        // 对join起来的数据进行拼接
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
                (PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>) tuple -> {
            String partAggrInfo = tuple._2._1;
            Row userInfoRow = tuple._2._2;
            String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

            int age = userInfoRow.getInt(3);
            String professional = userInfoRow.getString(4);
            String city = userInfoRow.getString(5);
            String sex = userInfoRow.getString(6);

            String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + String.valueOf(age) + "|"
                    + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                    + Constants.FIELD_CITY + "=" + city + "|"
                    + Constants.FIELD_SEX + "=" + sex;
            return new Tuple2<>(sessionid, fullAggrInfo);
        });

        return sessionid2FullAggrInfoRDD;
    }
}
