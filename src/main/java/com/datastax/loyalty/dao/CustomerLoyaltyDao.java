package com.datastax.loyalty.dao;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.loyalty.model.CustomerLoyalty;

/**
 * Inserts into 2 tables
 * 
 * @author patrickcallaghan
 *
 */
public class CustomerLoyaltyDao {

	private static Logger logger = LoggerFactory.getLogger(CustomerLoyaltyDao.class);
	private Session session;

	private static String keyspaceName = "datastax_loyalty";

	private static String pointsTable = keyspaceName + ".user_points";

	private static String INSERT_POINTS = "insert into " + pointsTable + " (id, time, value, comment) values (?,?,?,?);";
	private static String CREATE_CUSTOMER = "insert into " + pointsTable + " (id, time, balance, balanceat) values (?,?,?,?);";
	private static String GET_BALANCE = "select id, balance, balanceat from " + pointsTable + " where id = ?";
	private static String SUM_BALANCE = "select id, sum(value) as value from " + pointsTable + " where id = ? and time > ?";
	private static String UPDATE_BALANCE = "update " + pointsTable + " set balance=?, balanceat=? where id = ? if balance = ?";
	
	private Cluster cluster;
	
	private PreparedStatement createCustomer;
	private PreparedStatement sumBalance;
	private PreparedStatement insertPoints;
	private PreparedStatement updateBalance;
	private PreparedStatement getBalance;

	public CustomerLoyaltyDao(String[] contactPoints) {

		cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();
		
		this.createCustomer = session.prepare(CREATE_CUSTOMER);
		this.sumBalance = session.prepare(SUM_BALANCE);
		this.insertPoints = session.prepare(INSERT_POINTS);
		this.updateBalance = session.prepare(UPDATE_BALANCE);
		this.getBalance = session.prepare(GET_BALANCE);
	}

	public void insertPoints(CustomerLoyalty cust) {
		session.execute(insertPoints.bind("" + cust.getId(), cust.getTime(),cust.getValue(), cust.getComment()));
	}

	public void createCustomer(String custid, Date date) {
		session.execute(createCustomer.bind(custid, date, 10, date));
		session.execute(insertPoints.bind(custid, date, 10, "Starting Gift"));
	}

	public CustomerLoyalty getBalance(String custid) {
		ResultSet rs = session.execute(getBalance.bind(custid));
		CustomerLoyalty loyalty = new CustomerLoyalty();

		if (!rs.isExhausted()){
			Row row = rs.one();
			loyalty.setId(row.getString("id"));
			loyalty.setBalance(row.getInt("balance"));
			loyalty.setBalanceat(row.getTimestamp("balanceat"));
		}
		
		return loyalty;
	}
	
	public CustomerLoyalty sumBalance(String custid, Date date) {
		ResultSet rs = session.execute(sumBalance.bind(custid, date));
		CustomerLoyalty loyalty = new CustomerLoyalty();

		if (!rs.isExhausted()){
			Row row = rs.one();
			loyalty.setId(row.getString("id"));
			loyalty.setValue(row.getInt("value"));
		}
		
		return loyalty;
	}


	public boolean updateBalance(String id, int balance, Date balanceat, int oldBalance) {
		
		try {			
			ResultSetFuture resultSet = this.session.executeAsync(updateBalance.bind(balance, balanceat, id, oldBalance));
			
			if (resultSet != null) {
				Row row = resultSet.getUninterruptibly().one();
				boolean applied = row.getBool(0);
				
				if (!applied){
					logger.info("Update failed as balance is " + row.getInt(1) + " and not " + oldBalance);
					return false;
				}				
			}
		} catch (WriteTimeoutException e) {
			logger.warn(e.getMessage());
			return false;
		}		
		
		return true;
	}

	public boolean updateBalanceAndInsert(String id, int balance, Date balanceat, int oldBalance, CustomerLoyalty cust) {
		
		try {
			BatchStatement batch = new BatchStatement();
			batch.add(updateBalance.bind(balance, balanceat, id, oldBalance));
			batch.add(insertPoints.bind("" + cust.getId(), cust.getTime(),cust.getValue(), cust.getComment()));
			
			ResultSetFuture resultSet = this.session.executeAsync(batch);
			
			if (resultSet != null) {
				Row row = resultSet.getUninterruptibly().one();
				boolean applied = row.getBool(0);
				
				if (!applied){
					logger.info("Update failed as balance is " + row.getInt("balance") + " and not " + oldBalance);
					return false;
				}				
			}
		} catch (WriteTimeoutException e) {
			logger.warn(e.getMessage());
			return false;
		}		
		
		return true;
	}
}
