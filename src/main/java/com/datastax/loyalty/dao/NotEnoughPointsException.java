package com.datastax.loyalty.dao;

public class NotEnoughPointsException extends Exception {

	private static final long serialVersionUID = -3581145923123156443L;

	public NotEnoughPointsException(String message) {
		super(message);	
	}
}
