package com.datastax.loyalty.model;

import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace="datastax_loyalty", name="customer_points")
public class CustomerLoyalty {

	@PartitionKey
	private String id;
	@ClusteringColumn
	private Date time;

	private int balance;
	private Date balanceat;
	private int value; 
	private String comment;
	
	public CustomerLoyalty(){}
	
	public CustomerLoyalty(String id, Date time, int value, String comment) {
		super();
		this.id = id;
		this.time = time;
		this.value = value;
		this.comment = comment;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public int getBalance() {
		return balance;
	}
	public void setBalance(int balance) {
		this.balance = balance;
	}
	public Date getBalanceat() {
		return balanceat;
	}
	public void setBalanceat(Date balanceat) {
		this.balanceat = balanceat;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
	public String toString() {
		return "CustomerLoyalty [id=" + id + ", time=" + time + ", balance=" + balance + ", balanceat=" + balanceat
				+ ", value=" + value + ", comment=" + comment + "]";
	}
}
