package com.hft.sparkproject.spark;

import com.hft.sparkproject.conf.ConfigurationManager;
import com.hft.sparkproject.constant.Constants;
import com.hft.sparkproject.dao.ITaskDAO;
import com.hft.sparkproject.dao.factory.DAOFactory;
import com.hft.sparkproject.test.MockData;
import com.hft.sparkproject.util.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author : kai.wu
 * @date : 2019/2/19
 */
public class UserVisitSessionAnalyzeSparkTrain {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc);
        
        mockData(sc , sqlContext);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        Long taskid = ParamUtils.getTaskIdFromArgs(args);

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
}
