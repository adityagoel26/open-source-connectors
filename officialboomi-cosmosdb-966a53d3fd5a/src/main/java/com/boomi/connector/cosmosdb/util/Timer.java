//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.util;

/**
 * Logic to record time taken for input preparation and operation execution in MongoDB
 * @author abhijit.d.mishra
 */
public class Timer {
	
	private Timer() {
		throw new IllegalStateException("Utility class");
	}
	
	/** The timer output. */
	static long timerOutput;
	
	/** The start time. */
	static long startTime;
	
	/** The end time. */
	static long endTime;
	
	/**
	 * Resets timer
	 */
	private static void reset(){
		timerOutput=0;
	}
	
	/**
	 * Resets timer and starts timer
	 */
	public static void start(){
		reset();
		startTime = System.currentTimeMillis();
	}
	
	/**
	 * Stops timer and the elapsed time in ms from start
	 *
	 * @return the elapsed time from start
	 */
	public static long stop(){
		endTime = System.currentTimeMillis();
		timerOutput = endTime-startTime;
		return timerOutput;
	}

}
