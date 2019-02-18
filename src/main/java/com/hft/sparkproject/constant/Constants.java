package com.hft.sparkproject.constant;

import scala.tools.cmd.gen.AnyVals;

/**
 * 常量接口
 * @author Administrator
 */
public interface Constants {

    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String SPARK_LOCAL = "spark.local";

    /**
     * spark作业相关的常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
    String FIELD_SESSION_ID = "sessionid";
    String FIELD_SEARCH_KEY_WORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_AGE = "age";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";

    /**
     * 任务相关的常量
     */
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
}
