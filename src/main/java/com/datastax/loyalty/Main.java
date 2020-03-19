package com.datastax.loyalty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.ThreadUtils;
import com.datastax.demo.utils.Timer;
import com.datastax.loyalty.dao.CustomerLoyaltyDao;
import com.datastax.loyalty.model.CustomerLoyalty;
import com.datastax.loyalty.service.LoyaltyService;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public Main() {

		String noOfCustomersStr = PropertyHelper.getProperty("noOfCustomers", "5000");
		String noOfPointsStr = PropertyHelper.getProperty("noOfPoints", "500000");
		int noOfDays = Integer.parseInt(PropertyHelper.getProperty("noOfDays", "365"));
		
		BlockingQueue<CustomerLoyalty> queue = new ArrayBlockingQueue<CustomerLoyalty>(1000);
		List<KillableRunner> tasks = new ArrayList<>();
		
		//Executor for Threads
		int noOfThreads = Integer.parseInt(PropertyHelper.getProperty("noOfThreads", "1"));
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		
		LoyaltyService service = new LoyaltyService();

		int noOfCustomers = Integer.parseInt(noOfCustomersStr);
		int noOfPoints = Integer.parseInt(noOfPointsStr);

		logger.info("Writing " + noOfCustomers + " customers for " + noOfPoints + " points.");

		for (int i = 0; i < noOfThreads; i++) {
			
			KillableRunner task = new CustomerLoyaltyWriter(service, queue);
			executor.execute(task);
			tasks.add(task);
		}
		
		
		//Start Time
		logger.info("Creating customers");
		DateTime date = DateTime.now().minusDays(noOfDays);	
		for (int i = 0; i < noOfCustomers; i++) {
			service.createCustomer("U" + i);
		}
 		logger.info("Created customers");
		
		int interval = new Double(noOfDays * 86400000  / noOfPoints).intValue();
		Timer timer = new Timer();
		int count = 0;		
		
		for (int i = 0; i < noOfPoints; i++) {						
			//Add interval to date
			date = date.plusMillis(interval);
			String id;
			logger.info("date:" + date.toString());
			if (Math.random() < .1){
				id = "U" + new Double(Math.random()* 1000).intValue();				 			
			}else{
				id = "U" + new Double(Math.random()* noOfCustomers).intValue();
			}
				
			CustomerLoyalty custL;
	
			// create time by adding a random no of millis									
			if (Math.random() < .1){
				//Redeem
				custL = new CustomerLoyalty(id, date.toDate(), -5, "Redeem Coffee");
			}else{
				//Collect
				custL = new CustomerLoyalty(id, date.toDate(), 1, "Collect from Purchase");
			}
						
			try{
				queue.put(custL);
				
				if (++count % 10000 == 0){
					logger.info("Created " + count + " reward points");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}	
		}
		
		while(!queue.isEmpty()){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		timer.end();
		ThreadUtils.shutdown(tasks, executor);
		System.exit(0);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}

}
