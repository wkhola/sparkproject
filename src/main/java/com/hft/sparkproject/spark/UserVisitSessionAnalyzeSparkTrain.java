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
import com.hft.sparkproject.util.ValidUtils;
import org.apache.spark.SparkConf;
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

import java.util.Iterator;

/**
 * @author : kai.wu
 * @date : 2019/2/19
 */
public class UserVisitSessionAnalyzeSparkTrain {
    public static void main(String[] args) {
        args = new String[]{"1"};
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc);
        
        mockData(sc , sqlContext);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        Long taskid = ParamUtils.getTaskIdFromArgs(args);

        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);

        System.out.println(sessionid2AggrInfoRDD.count());
        for (Tuple2<String, String> tuple: sessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple._2);
        }

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSession(sessionid2AggrInfoRDD, taskParam);

        System.out.println(filteredSessionid2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple: filteredSessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple._2);
        }

        sc.close();
    }

    private static SQLContext getSQLContext(JavaSparkContext sc) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            return new SQLContext(sc.sc());
        }else {
            return new HiveContext(sc.sc());
        }
    }

    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            MockData.mock(sc, sqlContext);
        }

    }


    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * " +
                "from user_visit_action " +
                "where date >= '" + startDate + "' " +
                "and date <= '" + endDate + "'";

        DataFrame dataFrame = sqlContext.sql(sql);
        return dataFrame.javaRDD();
    }

    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext){
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String sessionid = row.getString(2);
                return new Tuple2<>(sessionid, row);
            }
        });
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {

                String sessionid = tuple2._1;

                Long userid = null;
                Iterator<Row> iterator = tuple2._2.iterator();

                StringBuffer searchKeyWordBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdBuffer = new StringBuffer("");
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    if(userid == null){
                        userid = row.getLong(1);
                    }
                    String searchKeyword = row.getString(5);
                    Long clickCategoryId = row.getLong(6);

                    if(StringUtils.isNotEmpty(searchKeyword)){
                        if(!searchKeyWordBuffer.toString().contains(searchKeyword)){
                            searchKeyWordBuffer.append(searchKeyword).append(",");
                        }
                    }
                    if(StringUtils.isNotEmpty(String.valueOf(clickCategoryId))){
                        if(!clickCategoryIdBuffer.toString().contains(String.valueOf(clickCategoryId))){
                            clickCategoryIdBuffer.append(clickCategoryId).append(",");
                        }
                    }
                }
                String searchKeywords = StringUtils.trimComma(String.valueOf(searchKeyWordBuffer));
                String clickCategoryIds = StringUtils.trimComma(String.valueOf(clickCategoryIdBuffer));
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                        + Constants.FIELD_SEARCH_KEY_WORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
                

                return new Tuple2<>(userid, partAggrInfo);
            }
        });

        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRow = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRow.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple2Tuple2) throws Exception {
                String partAggrInfo = tuple2Tuple2._2._1;
                Row userInfoRow = tuple2Tuple2._2._2;
                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,
                        Constants.DELIMITER, Constants.FIELD_SESSION_ID);
                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + String.valueOf(age) + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;
                return new Tuple2<>(sessionid, fullAggrInfo);
            }
        });

        return sessionid2FullAggrInfoRDD;
    }

    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                                             JSONObject taskParam){

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
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {

                String aggrInfo = tuple2._2;
                // 按照年龄进行过滤
                if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }

                // 按照职业范围进行过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }

                //按照城市进行过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                //按照性别进行过滤
                if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                //按照搜索词进行过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEY_WORDS, parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                // 按照点击品类
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }
                return true;
            }
        });
        return filteredSessionid2AggrInfoRDD;
    }
}
