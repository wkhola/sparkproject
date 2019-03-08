package com.hft.sparkproject.dao;

import com.hft.sparkproject.domain.SessionDetail;

import java.util.List;

/**
 * Session明细DAO接口
 * @author Administrator
 *
 */
public interface ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail  sessionDetail
	 */
	void insert(SessionDetail sessionDetail);
	
	/**
	 * 批量插入session明细数据
	 * @param sessionDetails sessionDetails
	 */
	void insertBatch(List<SessionDetail> sessionDetails);
	
}
