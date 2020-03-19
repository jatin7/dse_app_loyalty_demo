package com.datastax.loyalty.service;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.loyalty.dao.CustomerLoyaltyDao;
import com.datastax.loyalty.dao.NotEnoughPointsException;
import com.datastax.loyalty.model.CustomerLoyalty;

public class LoyaltyService {
	private static Logger logger = LoggerFactory.getLogger(LoyaltyService.class);
	private CustomerLoyaltyDao dao;

	public LoyaltyService(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "192.168.1.66");
		this.dao = new CustomerLoyaltyDao(contactPointsStr.split(","));
	}
	
	public void addPoints(String id, Date date, int points, String comment){
		
		CustomerLoyalty cust = new CustomerLoyalty(id, date, points, comment);
		
		dao.insertPoints(cust);
	}
	
	public void redeemPoints(String id, Date date, int points, String comment) throws NotEnoughPointsException{
		
		CustomerLoyalty cust = new CustomerLoyalty(id, date, -points, comment);
		
		redeem(cust);
	}

	public void redeem(CustomerLoyalty customerLoyalty) throws NotEnoughPointsException {
		// Redeem			
		
		//Update the balance before we redeem.
		CustomerLoyalty balance = this.dao.getBalance(customerLoyalty.getId());						
		CustomerLoyalty sumBalance = this.dao.sumBalance(customerLoyalty.getId(), balance.getBalanceat());

		logger.debug(balance.toString());
		logger.debug(sumBalance.toString());
		
		int currentBalance = balance.getBalance();
		int balanceSince = sumBalance.getValue();						
		int newBalance = currentBalance + balanceSince;
		
		//Update the balance to the current value before any redeem
		while (!dao.updateBalance(customerLoyalty.getId(), newBalance, customerLoyalty.getTime(), currentBalance)) {

			logger.info("Failed updating customer " + customerLoyalty.getId() + " with " + newBalance);

			// so read again and try to update
			balance = this.dao.getBalance(customerLoyalty.getId());
			sumBalance = this.dao.sumBalance(customerLoyalty.getId(), balance.getBalanceat());
			
			currentBalance = balance.getBalance();
			balanceSince = sumBalance.getValue();						
			newBalance = currentBalance + balanceSince;
		}
		
		//Balance when we redeem the points
		int redeemedBalance = newBalance + customerLoyalty.getValue();
		
		//If enough points - then redeem the balance. 
		if (redeemedBalance >= 0){						

			//If update fails, then the balance has changed since we last looked at it
			while (!dao.updateBalanceAndInsert(customerLoyalty.getId(), redeemedBalance, customerLoyalty.getTime(), newBalance, customerLoyalty)) {

				logger.debug("Failed updating customer " + customerLoyalty.getId() + " with " + redeemedBalance);
				
				//Need to recreate the balance and check redeemed balance. 
				balance = this.dao.getBalance(customerLoyalty.getId());
				newBalance = balance.getBalance();
				redeemedBalance = newBalance + customerLoyalty.getValue();
				
				if (redeemedBalance < 0){
					
					String msg = "Cannot redeem " + (-customerLoyalty.getValue()) + " points as balance = " + newBalance + " for customer " + customerLoyalty.getId();
					logger.info(msg);
					logger.info("Cannot redeem as balance would be less than 0");
					throw new NotEnoughPointsException(msg);					
				}								
			}
			this.dao.insertPoints(customerLoyalty);
		}else{
			String msg = "Cannot redeem " + (-customerLoyalty.getValue()) + " points as balance = " + newBalance + " for customer " + customerLoyalty.getId();
			throw new NotEnoughPointsException(msg);
		}
	}

	
	public void createCustomer(String id){
		
		dao.createCustomer(id, new Date());
	}
	
	public CustomerLoyalty getBalance(String id){
		return dao.getBalance(id);
	}

	public CustomerLoyalty sumBalance(String id, Date lastBalanceAt){
		return dao.sumBalance(id, lastBalanceAt);
	}

	public List<CustomerLoyalty> getHistory(String customerid) {
		return dao.getHistory(customerid);
	}
	
}

