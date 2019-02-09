package com.hft.sparkproject;

import com.hft.sparkproject.dao.ITaskDAO;
import com.hft.sparkproject.dao.factory.DAOFactory;
import com.hft.sparkproject.domain.Task;
import org.junit.Test;

/**
 * @author : kai.wu
 * @date : 2019/2/9
 */
public class DAOFactoryTest {

    @Test
    public void factoryTest(){
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task byId = taskDAO.findById(1);
        System.out.println(byId.getTaskName());
    }
}
