package com.tibco.metrics.bejmx;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Driver to collect BE performance statistics via JMX.
 * It supports 3 MBeans: Agent/Entity, Cache, and RTCTxnManagerReport. One or more MBean data can be printed out periodically.
 * Use a Java properties file to configure 1 or more Inference engines to be monitored.
 * Stat files are tagged by month-and-date, so every day, a new file is created for each BE engine and each MBean stats.
 * 
 * @author yxu
 */
public class BEJMX {
	
	// thread pool to fetch MBean data from multiple BE engines concurrently
	static ThreadPoolExecutor pool;
	
	// set to true if do not print out stats of BE internal objects
	static boolean ignoreInternalEntity = true;
	
	// full path of directory for all stat report files, null for current working directory
	static String reportFolder = null;
	
	// seconds to wait between consecutive MBean polls
	static int interval = 60;
	
	// all monitored engines in hash host:port -> JMXClient
	static HashMap<String, Client> clientMap = new HashMap<String, Client>();
	
	// all monitored statTypes, i.e., BEEntityCache, BEAgentEntity, and RTCTxnManagerReport
	// statType -> array of included entity patterns, only entities that match one of the patterns will be monitored.
	static HashMap<String, Set<String>> statTypes = new HashMap<String, Set<String>>();
	
	/**
	 * Main driver to start monitoring BE inference engines.
	 * @param args -config <config_file>
	 * 
	 * @throws Exception when failed to read/parse config file, or write stat report file.
	 */
	public static void main(String[] args) throws Exception
	{
		String configFile = null;
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-config")) {
				configFile = args[i + 1]; 
			}
			
			if (args.length < 2 || args[i].contains("-help") || args[i].equals("-?")) {
				printUsage();
				System.exit(0);
			}
		}
		
		System.out.println(getVersions());
		System.out.println("Starting BEJMX...");
		
		// load monitor properties from config file, and create JMX connections for all listed BE inference engines
		loadConfig(configFile);
		for (Client client : clientMap.values()) {
			initializeClient(client);
		}

		// create thread pool
		pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		pool.setKeepAliveTime(2*interval, TimeUnit.SECONDS);
		
		System.out.println("Start monitoring ...");
		boolean forever = true;
		while (forever) {
			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        	String timestamp = fmt.format(Calendar.getInstance().getTime());
			for (Client client : clientMap.values()) {
				client.setTimestamp(timestamp);
				pool.execute(new ClientThread(client));
			}
			System.out.println(String.format("%d of %d threads are active", pool.getActiveCount(), pool.getPoolSize()));
			try {
				TimeUnit.SECONDS.sleep(interval);
			} catch (InterruptedException e) {
				forever = false;
				shutdown();
			}
		}
	}
	
	/**
	 * Load configuration file, and make JMX connections to all configured BE inference engines
	 * 
	 * @param configFile full path of the configuration file
	 * @throws Exception when failed to read/parse configure file, or cannot connect to JMX
	 */
	private static void loadConfig(String configFile) throws Exception {
		System.out.println("Loading configuration from file " + configFile);
		Properties props = new Properties();
		FileInputStream fis = new FileInputStream(configFile);
		props.load(fis);
		fis.close();
		
		for (String key : props.stringPropertyNames()) {
			if (key.startsWith("engine.")) {
				if (key.startsWith("engine.jmxport.")) {
					String port = props.getProperty(key, "").trim();
					if (port.length() > 0) {
						String host = props.getProperty(key.replaceFirst("jmxport", "jmxhost"), "localhost").trim();
						String user = props.getProperty(key.replaceFirst("jmxport", "username"), "").trim();
						String passwd = props.getProperty(key.replaceFirst("jmxport", "password"), "").trim();
						String name = props.getProperty(key.replaceFirst("jmxport", "name"), "BE").trim();
						if (user.length() == 0 || passwd.length() == 0) {
							// do not use authentication if user or password is blank
							user = null;
							passwd = null;
						}
						String jmxKey = host + ":" + port;
						if (!clientMap.containsKey(jmxKey)) {
							// connect to JMX and initialize it for stat collection
							System.out.println(String.format("Connect to engine %s at %s:%s", name, host, port));
							Client c = new Client(name, host, Integer.parseInt(port), user, passwd);
							clientMap.put(jmxKey, c);
						}
					}
				}
			}
			else if (key.startsWith("report.")) {
				String type = props.getProperty(key, "").trim();
				if (type.length() > 0) {
					if (!statTypes.containsKey(type)) {
						// default no special includes, i.e., report all entities
						statTypes.put(type, null);
					}
				}
				System.out.println("Add report type " + type);
			}
			else if (key.startsWith("include.")) {
				// add included entity pattern to specified stat type
				String[] tokens = key.split("\\.");
				Set<String> includes = statTypes.get(tokens[1]);
				if (null == includes) {
					includes = new HashSet<String>();
					statTypes.put(tokens[1], includes);
				}
				String pattern = props.getProperty(key, "").trim();
				if (pattern.length() > 0) {
					includes.add(pattern);
				}
				System.out.println(String.format("Report %s includes entity pattern %s", tokens[1], pattern));
			}
			else if (key.equals("interval")) {
				interval = Integer.parseInt(props.getProperty(key, "30").trim());
				System.out.println("Write stats every " + interval + " seconds");
			}
			else if (key.equals("ignoreInternalEntity")) {
				ignoreInternalEntity = Boolean.parseBoolean(props.getProperty(key, "false").trim());
				if (ignoreInternalEntity) {
					System.out.println("Ignore stats of BE internal entities");
				}
			}
			else if (key.equals("reportFolder")) {
				reportFolder = props.getProperty(key, "").trim();
				if (0 == reportFolder.length()) {
					reportFolder = null;
				}
				System.out.println("Statistics report is in folder " + reportFolder);
			}
			else {
				System.out.println("ignore config property " + key);
			}
		}
	}
	
	/**
	 * Initialize JMX client with configured parameters, i.e., MBean types for statistics collection, and output folder for stta reports.
	 * 
	 * @param client JMX client to be initialized
	 */
	private static void initializeClient(Client client) {
		client.setReportFolder(reportFolder);
		String[] statArray = new String[statTypes.size()];
		statTypes.keySet().toArray(statArray);
		client.setStatTypes(statArray);		
	}
	
	/**
	 * Gracefully shutdown the thread pool, close all JMX connections.
	 */
	private static void shutdown() {
		System.out.println("Shutting down ...");
		pool.shutdown();
		for (Client client : clientMap.values()) {
			client.cleanup();
		}

		// wait until all threads complete
		try {
			if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
				System.out.println("Force shutdown after 30 seconds ...");
				pool.shutdownNow();
				if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
					System.out.println("Terminate the process.");
					Thread.currentThread().interrupt();
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * print out usage info for the monitoring commands
	 */
	public static void printUsage()
	{
		System.out.println(getVersions());
		System.out.println("Collect BE Metrics about Cache, Agent, and RTC");
		System.out.println("BEJMX Usage:");
		System.out.println("java com.tibco.metrics.bejmx.BEJMX -config <configFile>");
	}
	
	/**
	 * return version banner of this monitoring tool
	 * @return
	 */
	public static  String getVersions() {
		String banner =  "\n\t *********************************************************************************" +
		 				 "\n\t JMX Collection Metrics for TIBCO BusinessEvents 5.x (2015-07-24)"+		 				 
				         "\n\t Copyright 2015 TIBCO Software Inc.  " +
				         "\n\t All rights reserved."+
				         "\n\t **********************************************************************************\n";
		return banner;
	}	

	/**
	 * Configured rules for filtering out entities that are not wanted in the stat report.
	 * 
	 * @param attrName name of an entity to be evaluated for stat reporting.
	 * @param statType type of the MBean data to be reported.
	 * @return true if the entity is filtered out, and thus not tracked, false otherwise.
	 */
	public static boolean isIgnoredEntity(String attrName, String statType) {
		// ignore ObjectTableIds
		if (attrName.endsWith("--ObjectTableIds")) {
			return true;
		} 
		else if (ignoreInternalEntity && attrName.contains("com.tibco.cep.runtime.model")) {
			return true;
		}
		
		Set<String> includedPatterns = statTypes.get(statType);
		if (includedPatterns != null && includedPatterns.size() > 0) {
			for (String pattern : includedPatterns) {
				if (attrName.matches(pattern)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
}
