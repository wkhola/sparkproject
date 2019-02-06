package com.hft.sparkproject.dao.factory;

import com.hft.sparkproject.dao.ITaskDAO;
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
}
