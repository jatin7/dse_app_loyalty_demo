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
					
					if (customerLoyalty.getValue() < 0) {

						// Redeem						
						CustomerLoyalty balance = this.dao.getBalance(customerLoyalty.getId());						
						CustomerLoyalty sumBalance = this.dao.sumBalance(customerLoyalty.getId(), balance.getBalanceat());

						//logger.info(balance.toString());
						//logger.info(sumBalance.toString());
						
						int currentBalance = balance.getBalance();
						int balanceSince = sumBalance.getValue();						
						int newBalance = currentBalance + balanceSince;
						
						// If we can't update, then someone else has used it
						//logger.info("updating " + currentBalance + " to " + newBalance + " at "  + customerLoyalty.getTime() );
						
						//Update the balance to the current value before any redeem
						while (!dao.update(customerLoyalty.getId(), newBalance, customerLoyalty.getTime(), currentBalance)) {

							logger.info("Failed updating customer " + customerLoyalty.getId() + " with " + newBalance);

							// so read again and try to update
							balance = this.dao.getBalance(customerLoyalty.getId());
							sumBalance = this.dao.sumBalance(customerLoyalty.getId(), balance.getBalanceat());
							
							currentBalance = balance.getBalance();
							balanceSince = sumBalance.getValue();						
							newBalance = currentBalance + balanceSince;
						}
						
						int redeemedBalance = newBalance + customerLoyalty.getValue();
						
						//If enough points - then redeem the balance. 
						if (redeemedBalance >= 0){						

							//logger.info("updating " + newBalance + " to " + redeemedBalance + " at "  + customerLoyalty.getTime() );
							while (!dao.update(customerLoyalty.getId(), redeemedBalance, customerLoyalty.getTime(), newBalance, customerLoyalty)) {

								logger.info("Failed updating customer " + customerLoyalty.getId() + " with " + redeemedBalance);
								
								balance = this.dao.getBalance(customerLoyalty.getId());
								newBalance = balance.getBalance();
								redeemedBalance = newBalance + customerLoyalty.getValue();
							}
							this.dao.insertPoints(customerLoyalty);
						}else{
							logger.info("Cannot redeem " + (-customerLoyalty.getValue()) + " points as balance = " + newBalance);
						}
						
					}else{
						this.dao.insertPoints(customerLoyalty);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void shutdown() {
		while (!queue.isEmpty())
			logger.info("Finishing");
			shutdown = true;
	}
}
