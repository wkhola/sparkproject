package com.hft.sparkproject.dao;

import com.hft.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.hft.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.hft.sparkproject.dao.impl.TaskDAOImpl;

/**
 * DAO 工厂类
 * @author Administrator
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return DAO
     */
    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

}
