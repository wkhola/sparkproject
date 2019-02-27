package com.hft.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.hft.sparkproject.conf.ConfigurationManager;
import com.hft.sparkproject.constant.Constants;
import com.hft.sparkproject.dao.ITaskDAO;
import com.hft.sparkproject.dao.factory.DAOFactory;
import com.hft.sparkproject.domain.Task;
import com.hft.sparkproject.test.MockData;
import com.hft.sparkproject.util.DateUtils;
import com.hft.sparkproject.util.ParamUtils;
import com.hft.sparkproject.util.StringUtils;
import com.hft.sparkproject.util.ValidUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * 用户访问session分析spark作业
 * @author : kai.wu
 * @date : 2019/2/9
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        args = new String[]{"1"};
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
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);

        System.out.println(sessionid2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple: sessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple._2);
        }

        sessionid2AggrInfoRDD.count();
        Accumulator<String> sessionAggrAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD,
                taskParam, sessionAggrAccumulator);

        System.out.println(filteredSessionid2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple: filteredSessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple._2);
        }
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

            // session的起始时间
            Date startTime = null;
            Date endTime = null;

            // session的步长
            int stepLength = 0;

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

                // 计算session开始和结束时间
                Date actionTime = DateUtils.parseTime(row.getString(4));

                if(startTime == null) {
                    startTime = actionTime;
                }
                if(endTime == null) {
                    endTime = actionTime;
                }

                if (actionTime != null) {
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                }
                stepLength++;
            }
            String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
            String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

            long visitLength = 0;
            if (endTime != null) {
                visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
            }
            String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
            + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
            + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
            + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
            + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
            + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
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

    /**
     * 过滤session
     * @param sessionid2AggrInfoRDD RDD
     * @return javaPairRDD
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                                                        JSONObject taskParam,
                                                                        Accumulator<String> sessionAggrStatAccumulator){
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String aggrParameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
        if(aggrParameter.endsWith(Constants.DELIMITER)){
            aggrParameter = aggrParameter.substring(0, aggrParameter.length() - 1);
        }

        String parameter = aggrParameter;
        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {

                    private static final long serialVersionUID = -1510146875988055813L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {

                        String aggrInfo = tuple._2;


                        //按照年龄进行过滤
                        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
                            return false;
                        }

                        //按照职业范围进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)){
                            return false;
                        }

                        //按照城市进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)){
                            return false;
                        }

                        //按照性别进行过滤
                        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)){
                            return false;
                        }

                        //按照搜索词进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)){
                            return false;
                        }

                        // 按照点击品类
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)){
                            return false;
                        }

                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        String visitLengthStr = StringUtils.getFieldFromConcatString(aggrInfo, Constants.DELIMITER,
                                Constants.FIELD_VISIT_LENGTH);
                        String stepLengthStr = StringUtils.getFieldFromConcatString(
                                aggrInfo, Constants.DELIMITER, Constants.FIELD_STEP_LENGTH);

                        calVisitLength(visitLengthStr);
                        calStepLength(stepLengthStr);
                        return true;
                    }

                    private void calVisitLength(String visitLengthStr){
                        if (visitLengthStr != null){
                            long visitLength = Long.valueOf(visitLengthStr);
                            if(visitLength >= 1 && visitLength <= 3) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1S_3S);
                            } else if(visitLength >= 4 && visitLength <= 6) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4S_6S);
                            } else if(visitLength >= 7 && visitLength <= 9) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7S_9S);
                            } else if(visitLength >= 10 && visitLength <= 30) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10S_30S);
                            } else if(visitLength > 30 && visitLength <= 60) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30S_60S);
                            } else if(visitLength > 60 && visitLength <= 180) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1M_3M);
                            } else if(visitLength > 180 && visitLength <= 600) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3M_10M);
                            } else if(visitLength > 600 && visitLength <= 1800) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10M_30M);
                            } else if(visitLength > 1800) {
                                sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30M);
                            }
                        }
                    }

                    private void calStepLength(String stepLengthStr) {
                        if (stepLengthStr != null){
                            long stepLength = Long.valueOf(stepLengthStr);
                            if(stepLength >= 1 && stepLength <= 3) {
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                            } else if(stepLength >= 4 && stepLength <= 6) {
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                            } else if(stepLength >= 7 && stepLength <= 9) {
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                            } else if(stepLength >= 10 && stepLength <= 30) {
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                            } else if(stepLength > 30 && stepLength <= 60) {
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                            } else if(stepLength > 60) {
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                            }
                        }
                    }
        });
        return filteredSessionid2AggrInfoRDD;
    }
}
