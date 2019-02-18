package com.hft.sparkproject;

import com.hft.sparkproject.dao.DAOFactory;
import com.hft.sparkproject.dao.ITaskDAO;
import com.hft.sparkproject.domain.Task;
import org.junit.Test;

public class TaskDAOTest {

    @Test
    public void taskDAOTest(){
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task.getTaskName());
    }

}
