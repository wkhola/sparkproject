package com.hft.sparkproject.dao.factory;

import com.hft.sparkproject.dao.ISessionAggrStatDAO;
import com.hft.sparkproject.dao.ISessionRandomExtractDAO;
import com.hft.sparkproject.dao.ITaskDAO;
import com.hft.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.hft.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.hft.sparkproject.dao.impl.TaskDAOImpl;

/**
 * DAO 管理类
 * @author : kai.wu
 * @date : 2019/2/7
 */
public class DAOFactory {

    /**
     * 获取任务管理dao
     * @return DAO
     */
    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
        return new SessionRandomExtractDAOImpl();
    }
}
