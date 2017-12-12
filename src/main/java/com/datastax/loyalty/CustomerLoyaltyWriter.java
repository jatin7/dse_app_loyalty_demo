package com.datastax.loyalty;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.loyalty.dao.CustomerLoyaltyDao;
import com.datastax.loyalty.model.CustomerLoyalty;

public class CustomerLoyaltyWriter implements KillableRunner {

	private static Logger logger = LoggerFactory.getLogger(CustomerLoyaltyWriter.class);
	private volatile boolean shutdown = false;
	private CustomerLoyaltyDao dao;
	private BlockingQueue<CustomerLoyalty> queue;

	public CustomerLoyaltyWriter(CustomerLoyaltyDao dao, BlockingQueue<CustomerLoyalty> queue) {
		this.dao = dao;
		this.queue = queue;
	}

	@Override
	public void run() {
		CustomerLoyalty customerLoyalty;
		while (!shutdown) {
			customerLoyalty = queue.poll();

			if (customerLoyalty != null) {
				try {
					
					//Redeem will have a minus value on the customers loyalty acccount. 
					if (customerLoyalty.getValue() < 0) {

						redeem(customerLoyalty);
						
					}else{
						this.dao.insertPoints(customerLoyalty);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void redeem(CustomerLoyalty customerLoyalty) {
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
					logger.info("Cannot redeem " + (-customerLoyalty.getValue()) + " points as balance = " + newBalance + " for customer " + customerLoyalty.getId());
					logger.info("Cannot redeem as balance would be less than 0");
					return;
				}								
			}
			this.dao.insertPoints(customerLoyalty);
		}else{
			logger.info("Cannot redeem " + (-customerLoyalty.getValue()) + " points as balance = " + newBalance + " for customer " + customerLoyalty.getId());
		}
	}

	@Override
	public void shutdown() {
		while (!queue.isEmpty())
			logger.info("Finishing");
			shutdown = true;
	}
}
