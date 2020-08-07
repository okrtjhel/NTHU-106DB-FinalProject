package org.vanilladb.bench;

import org.vanilladb.bench.benchmarks.micro.MicroBenchmarker;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcDriver;
import org.vanilladb.bench.remote.sp.VanillaDbSpDriver;

public class App {
	
	private static int action;
	
	public static void main(String[] args) {
		Benchmarker benchmarker = null;
		
		try {
			parseArguments(args);
		} catch (IllegalArgumentException e) {
			System.err.println("Error: " + e.getMessage());
			System.err.println("Usage: ./app [Action]");
		}
		
		// Create a driver for connection
		SutDriver driver = null;
		switch (BenchmarkerParameters.CONNECTION_MODE) {
		case JDBC:
			driver = new VanillaDbJdbcDriver();
			break;
		case SP:
			driver = new VanillaDbSpDriver();
			break;
		}
		
		// Create a benchmarker
		benchmarker = new MicroBenchmarker(driver);
		
		switch (action) {
		case 1: // Load testbed
			benchmarker.loadTestbed();
			break;
		case 2: // Benchmarking
			benchmarker.benchmark();
			break;
		}
	}
	
	private static void parseArguments(String[] args) throws IllegalArgumentException {
		if (args.length < 1) {
			throw new IllegalArgumentException("The number of arguments is less than 1");
		}
		
		try {
			action = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(String.format("'%s' is not a number", args[0]));
		}
	}
}
