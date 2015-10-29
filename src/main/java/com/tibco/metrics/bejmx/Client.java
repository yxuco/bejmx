/*
 * Client.java - JMX client that interacts with the JMX agent in one BE engine.
 * It can fetch multiple MBean stats, and it writes each set of stats to a different report file.
 * Name of report file is generated based on the BE engine properties, and report file is rolling files with size limit.
 */

package com.tibco.metrics.bejmx;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Main JMX Client class.
 *
 */
public class Client {

	protected JMXConnector jmxc = null;
	protected MBeanServerConnection mbsc = null;

	private String engineName;
	private String host;
	private int port;
	private int pid = -1;
	private String username;
	private String password;
	private HashMap<String, FileWriter> writerMap;
	private HashMap<String, String> fileMap;

	private String timestamp;

	// parameters to set for writing stat files in separate thread
	private String reportFolder = null;
	private String[] statTypes = new String[] { "BEAgentEntity", "BEEntityCache", "RTCTxnManagerReport" };

	// ClassName=BEEntityCache: com.tibco.be/Cache/<concept or event> Attributes
	// (print only with CacheSize > 0)
	static String[] BEEntityCachereportCols = { "ClassName", "DateTime", "CacheSize", "GetAvgTime", "GetCount",
			"NumHandlesInStore", "PutAvgTime", "PutCount", "RemoveAvgTime", "RemoveCount", "TypeId" };

	// ClassName=RTCTxnManagerReport: com.tibco.be/RTCTxnManagerReport
	// Attributes
	static String[] BERTCTxnManagerReport = { "DateTime", "AvgActionTxnMillis", "AvgCacheQueueWaitTimeMillis",
			"AvgCacheTxnMillis", "AvgDBOpsBatchSize", "AvgDBQueueWaitTimeMillis", "AvgDBTxnMillis",
			"AvgSuccessfulTxnTimeMillis", "LastDBBatchSize", "PendingActions", "PendingCacheWrites", "PendingDBWrites",
			"PendingEventsToAck", "PendingLocksToRelease", "TotalDBTxnsCompleted", "TotalErrors",
			"TotalSuccessfulTxns" };

	// ClassName=BEAgentEntity: com.tibco.be/Agent/1/Entity/<concept or event>
	// (print only with NumAssertedFromChannel > 0)
	static String[] BEAgentEntityReport = { "DateTime", "AvgTimeInRTC", "AvgTimePostRTC", "AvgTimePreRTC", "CacheMode",
			"NumAssertedFromAgents", "NumAssertedFromChannel", "NumHitsInL1Cache", "NumMissesInL1Cache",
			"NumModifiedFromAgents", "NumModifiedFromChannel", "NumRecovered", "NumRetractedFromAgents",
			"NumRetractedFromChannel" };

	// TODO: add channel destination attributes
	// com.tibco.be/Methods/Channels GetChannels(null), GetDestinations(channel,
	// null)
	// Attributes: Channel URI, Destination URI, Num Events Received, Num Events
	// Sent, Received Events Rate, Received Events Rate in last stats interval,
	// Suspended

	/**
	 * Construct a JMX client to connect to a BE engine. MBean server does not
	 * require security check.
	 *
	 * @param engineName
	 *            name of the engine to collect statistics from
	 * @param host
	 *            name of the host of the BE engine
	 * @param port
	 *            JMX port of the BE engine
	 */
	public Client(String engineName, String host, int port) {
		this(engineName, host, port, null, null);
	}

	/**
	 * Construct a JMX client to connect to a BE engine. MBean server is
	 * protected by user and password.
	 *
	 * @param engineName
	 *            name of the engine to collect statistics from
	 * @param host
	 *            name of the host of the BE engine
	 * @param port
	 *            JMX port of the BE engine
	 * @param username
	 *            user to login for MBean
	 * @param password
	 *            password for the JMX connection
	 */
	public Client(String engineName, String host, int port, String username, String password) {
		this.engineName = engineName;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.writerMap = new HashMap<String, FileWriter>();
		this.fileMap = new HashMap<String, String>();
	}

	public Client(int pid) {
		this.pid = pid;
		this.engineName = "PID-" + pid;
		this.writerMap = new HashMap<String, FileWriter>();
		this.fileMap = new HashMap<String, String>();
	}

	@SuppressWarnings("restriction")
	private void openConnection() throws IOException {
		// connect to MBean server
		String urlStr = null;
		HashMap<String, String[]> env = null;
		if (pid != -1) {
			// local Java process
			urlStr = sun.management.ConnectorAddressLink.importFrom(pid);
			if (null == urlStr) {
				System.out.println(String.format("No JMX address for pid %s, attach to VM and load agent jar", pid));
				loadAgentJar();
				urlStr = sun.management.ConnectorAddressLink.importFrom(pid);
			}
		} else {
			// remote MBean server
			if (username != null) {
				env = new HashMap<String, String[]>();
				env.put("jmx.remote.credentials", new String[] { username, password });
			}
			urlStr = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", host, port);
		}
		System.out.println(
				String.format("Connect to engine %s on JMX url %s with user %s ", engineName, urlStr, username));
		JMXServiceURL url = new JMXServiceURL(urlStr);
		jmxc = JMXConnectorFactory.connect(url, env);
		mbsc = jmxc.getMBeanServerConnection();
	}

	private void loadAgentJar() throws IOException {
		String javaHome = System.getProperty("java.home");
		File agentJar = new File(javaHome + File.separator + "lib" + File.separator + "management-agent.jar");
		if (agentJar.exists()) {
			String agentJarPath = agentJar.getCanonicalPath();
			try {
				com.sun.tools.attach.VirtualMachine.attach(String.valueOf(pid)).loadAgent(agentJarPath);
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException(e.getMessage());
			}
		}
	}

	/**
	 * Set folder name for stat log files
	 *
	 * @param reportFolder
	 *            name of the folder to write stats
	 */
	public void setReportFolder(String reportFolder) {
		this.reportFolder = reportFolder;
	}

	/**
	 * Set list of types required to generate reports.
	 *
	 * @param statTypes
	 *            one or more supported stats, i.e., BEEntityCache,
	 *            BEAgentEntity, RTCTxnManagerReport
	 */
	public void setStatTypes(String[] statTypes) {
		this.statTypes = statTypes;
	}

	/**
	 * Find or create a writer for stat file of a specified type
	 *
	 * @param statType
	 *            type of statistics, currently supports BEAgentEntity,
	 *            BEEntityCache, RTCTxnManagerReport (default)
	 * @return
	 * @throws IOException
	 *             when failed to create stat file
	 */
	private FileWriter getWriter(String statType) throws IOException {
		FileWriter writer = writerMap.get(statType);
		if (writer != null) {
			String filename = fileMap.get(statType);
			if (statFilename(statType).equals(filename)) {
				return writer;
			} else {
				// start a new day, so close the old writer
				closeWriter(statType);
			}
		}

		// return a new file writer
		return createWriter(statType);
	}

	public void closeWriter(String statType) {
		FileWriter writer = writerMap.get(statType);
		if (writer != null) {
			try {
				System.out.println(String.format("Close writer for %s on connection %s:%s", statType, host, port));
				writer.close();
			} catch (IOException io) {
				// do nothing
			}
		}
		writerMap.remove(statType);
		fileMap.remove(statType);
	}

	/**
	 * Create a file and pre-configured report folder for writing stat logs of a
	 * given type
	 *
	 * @param statType
	 *            type of statistics, currently supports BEAgentEntity,
	 *            BEEntityCache, RTCTxnManagerReport (default)
	 *
	 * @return file writer to append text data
	 * @throws IOException
	 */
	private FileWriter createWriter(String statType) throws IOException {
		FileWriter writer = null;
		File folder = null;
		if (reportFolder != null) {
			folder = new File(reportFolder);
			if (!folder.exists()) {
				boolean ok = folder.mkdirs();
				if (!ok) {
					throw new IOException("Failed to create directory " + reportFolder);
				}
			}
		}

		String filename = statFilename(statType);
		File statFile = new File(folder, filename);
		boolean isNew = !statFile.exists();
		writer = new FileWriter(statFile, true); // file for append
		if (isNew) {
			// write header as the first line of new file
			writer.write(getHeader(statType));
		}

		// cache the writer for data or cleanup
		writerMap.put(statType, writer);
		fileMap.put(statType, filename);
		return writer;
	}

	private String statFilename(String statType) {
		Calendar cal = Calendar.getInstance();
		if (pid != -1) {
			return String.format("%s_%s_%3$tm_%3$td.csv", engineName, statType, cal);
		} else {
			return String.format("%s_%s_%s_%s_%5$tm_%5$td.csv", engineName, host, port, statType, cal);
		}
	}

	/**
	 * Query MBean to get list of objects under /Agent/<agent-id>/Entity
	 *
	 * @return set of object names for agent entities
	 *
	 * @throws Exception
	 */
	private Set<ObjectName> getAgentEntityList() throws Exception {
		return mbsc.queryNames(new ObjectName("com.tibco.be:type=Agent,agentId=*,subType=Entity,entityId=*"), null);
	}

	/**
	 * Query MBean to get list of objects under /Cache
	 *
	 * @return set of object names for cache entities
	 *
	 * @throws Exception
	 */
	private Set<ObjectName> getCacheList() throws Exception {
		return mbsc.queryNames(new ObjectName("com.tibco.be:service=Cache,name=*"), null);
	}

	/**
	 * Query MBean to get MBean of name RTCTxnManagerReport
	 *
	 * @return one object name for RTCTxnManagerReport, wrapped as Set to match
	 *         other MBean's
	 *
	 * @throws Exception
	 */
	private Set<ObjectName> getRTCTxnList() throws Exception {
		return mbsc.queryNames(new ObjectName("com.tibco.be:service=RTCTxnManagerReport"), null);
	}

	/**
	 * reset stats for RTCTxnManagerReport, so the next call returns a delta
	 * stat
	 *
	 * @throws IOException
	 */
	private void resetRTCTxnStats() throws IOException {
		try {
			ObjectName mbeanName = new ObjectName("com.tibco.be:service=RTCTxnManagerReport");
			mbsc.invoke(mbeanName, "resetStats", null, null);
		} catch (Exception e) {
			throw new IOException("Failed to reset stats for RTCTxnManagerReport: " + e.getMessage());
		}
		/*
		 * RTCTxnManagerReportMBean mbeanProxy = JMX.newMBeanProxy(mbsc,
		 * mbeanName, RTCTxnManagerReportMBean.class, true); // reset the stats
		 * to prepare for the next call mbeanProxy.resetStats();
		 */
	}

	/**
	 * Query MBean to collect list of attribute of a specified object name.
	 *
	 * @param objName
	 *            object name of a watched entity
	 *
	 * @return attribute name and value map
	 *
	 * @throws Exception
	 */
	private Map<String, Object> getMBeanAttributes(ObjectName objName) throws Exception {
		MBeanInfo info = mbsc.getMBeanInfo(objName);
		MBeanAttributeInfo[] attrInfo = info.getAttributes();
		String[] attrNames = new String[attrInfo.length];

		for (int i = 0; i < attrNames.length; i++) {
			attrNames[i] = attrInfo[i].getName();
		}

		AttributeList list = mbsc.getAttributes(objName, attrNames);
		Map<String, Object> attrMap = new HashMap<String, Object>();

		for (Attribute attr : list.asList()) {
			attrMap.put(attr.getName(), attr.getValue());
		}
		return attrMap;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Collect all pre-configured stats, and write them to stat log files. can
	 * be called by separate worker threads.
	 *
	 * @throws IOException
	 *             when failed to write stat file
	 */
	public void writeAllMetrics() {
		if (null == timestamp) {
			// should not be here, just in case.
			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
			timestamp = fmt.format(Calendar.getInstance().getTime());
		}
		if (statTypes != null) {
			if (null == jmxc) {
				try {
					// reconnect to JMX
					openConnection();
				} catch (IOException e) {
					e.printStackTrace();
					System.out
							.println(String.format("Failed to connect to engine %s @ %s:%s ", engineName, host, port));
					closeConnection();
					timestamp = null;
					return;
				}
			}
			for (String statType : statTypes) {
				try {
					writeMetrics(statType, timestamp);
				} catch (IOException e) {
					// close old file and try again with new writer
					System.out.println(
							String.format("Failed to write %s from %s:%s %s", statType, host, port, e.getMessage()));
					closeWriter(statType);
					try {
						writeMetrics(statType, timestamp);
					} catch (IOException io) {
						System.out.println(
								String.format("Exception while writing stat for %s: %s", statType, io.getMessage()));
						closeWriter(statType);
					}
				}
			}
		}
		timestamp = null;
	}

	/**
	 * Collect MBean data of a specified type, and write data to pre-configured
	 * log file
	 *
	 * @param statType
	 *            type of statistics, currently supports BEAgentEntity,
	 *            BEEntityCache, RTCTxnManagerReport (default)
	 * @throws IOException
	 *             when failed to write stat data to file
	 */
	public void writeMetrics(String statType, String timestamp) throws IOException {
		FileWriter writer = getWriter(statType);

		Set<ObjectName> list = null;
		// query MBean for list of entities
		try {
			if ("BEEntityCache".equals(statType)) {
				list = getCacheList();
			} else if ("BEAgentEntity".equals(statType)) {
				list = getAgentEntityList();
			} else {
				list = getRTCTxnList();
			}
		} catch (Exception e) {
			System.out.println(String.format("Failed to get entity list for %s: %s\n", statType, e.getMessage()));
			closeConnection();
		}

		if (null == list || 0 == list.size()) {
			writer.write(String.format("Entity list for %s is empty", statType));
			return;
		}

		// query MBean for attributes of each entity
		for (ObjectName on : list) {
			String name = null;
			try {
				if ("BEEntityCache".equals(statType)) {
					name = on.getKeyProperty("name");
				} else if ("BEAgentEntity".equals(statType)) {
					name = on.getKeyProperty("entityId");
				} else {
					name = statType;
				}
				Map<String, Object> attrMap = getMBeanAttributes(on);
				attrMap.put("DateTime", timestamp);
				String row = serializeMetrics(statType, name, attrMap);
				writer.write(row);

				// reset stats
				if ("RTCTxnManagerReport".equals(statType)) {
					resetRTCTxnStats();
				}
			} catch (Exception ex) {
				writer.write(String.format("Failed to get attributes for entity %s: %s\n", name, ex.getMessage()));
			}
		}
		writer.flush();

		// throw exception if file becomes stale, so the writer is closed and
		// re-created
		checkFile(statType);
	}

	/**
	 * Check if the file still exists and is writable. Linux allows the system
	 * to continue to write even if file is deleted. So, this check will throw
	 * exception, which will lead to closing and re-creating the file writer.
	 *
	 * @param statType
	 *            type of the report
	 * @throws IOException
	 *             when the report file for the stat type no longer exist or not
	 *             writable.
	 */
	private void checkFile(String statType) throws IOException {
		File statFile = new File(reportFolder, statFilename(statType));
		if (!statFile.exists() || !statFile.canWrite()) {
			throw new IOException(String.format("File %s no longer exist", statFile.getAbsolutePath()));
		}
	}

	/**
	 * Write header string for stats of a speciied type into a pre-configured
	 * stat log file
	 *
	 * @param statType
	 *            type of statistics, currently supports BEAgentEntity,
	 *            BEEntityCache, RTCTxnManagerReport (default)
	 */
	public String getHeader(String statType) {
		StringBuilder str = new StringBuilder();
		if (statType.equals("BEEntityCache")) {
			str.append(BEEntityCachereportCols[0]);
			for (int idx = 1; idx < BEEntityCachereportCols.length; idx++) {
				str.append(',');
				str.append(BEEntityCachereportCols[idx]);
			}
		} else if (statType.equals("BEAgentEntity")) {
			str.append("Object,");
			str.append(BEAgentEntityReport[0]);
			for (int idx = 1; idx < BEAgentEntityReport.length; idx++) {
				str.append(',');
				str.append(BEAgentEntityReport[idx]);
			}
		} else {
			str.append("Object,");
			str.append(BERTCTxnManagerReport[0]);
			for (int idx = 1; idx < BERTCTxnManagerReport.length; idx++) {
				str.append(',');
				str.append(BERTCTxnManagerReport[idx]);
			}
		}
		str.append('\n');
		return str.toString();
	}

	/**
	 * Convert MBean attributes of a monitored entity into string
	 *
	 * @param statType
	 *            type of statistics, currently supports BEAgentEntity,
	 *            BEEntityCache, RTCTxnManagerReport (default)
	 * @param name
	 *            name of the monitored entity, e.g., concept of event
	 * @param attrs
	 *            statistic data in name-value pairs.
	 * @return resulting string containing the statistic data
	 * @throws IOException
	 */
	private String serializeMetrics(String statType, String name, Map<String, Object> attrs) throws IOException {
		StringBuilder rec = new StringBuilder();

		if ("BEAgentEntity".equals(statType)) {
			String cname = name;
			if (cname != null && cname.startsWith("be.gen.")) {
				cname = cname.substring(7);
			}
			if (!BEJMX.isIgnoredEntity(cname, statType)) {
				rec.append(cname);
				for (int idx = 0; idx < BEAgentEntityReport.length; idx++) {
					rec.append(',');
					rec.append(attrs.get(BEAgentEntityReport[idx]));
				}
				rec.append('\n');
			}
		} else if ("BEEntityCache".equals(statType)) {
			String cname = (String) attrs.get(BEEntityCachereportCols[0]);
			if (cname != null && cname.startsWith("be.gen.")) {
				cname = cname.substring(7);
			}
			if (cname != null && !BEJMX.isIgnoredEntity(cname, statType)) {
				rec.append(cname);
				for (int idx = 1; idx < BEEntityCachereportCols.length; idx++) {
					rec.append(',');
					rec.append(attrs.get(BEEntityCachereportCols[idx]));
				}
				rec.append('\n');
			}
		} else {
			// default to RTCTxnManagerReport
			rec.append(name);
			for (int idx = 0; idx < BERTCTxnManagerReport.length; idx++) {
				rec.append(',');
				rec.append(attrs.get(BERTCTxnManagerReport[idx]));
			}
			rec.append('\n');
		}

		return rec.toString();
	}

	private void closeConnection() {
		if (null == jmxc) {
			return;
		}
		try {
			jmxc.close();
		} catch (IOException e) {
			// do nothing
		}
		jmxc = null;
		mbsc = null;
	}

	/**
	 * Close JMX connection, and close log file writers
	 *
	 * @throws IOException
	 */
	public void cleanup() {
		closeConnection();
		for (FileWriter writer : writerMap.values()) {
			try {
				writer.close();
			} catch (IOException e) {
				// do nothing
			}
		}
		writerMap.clear();
		fileMap.clear();
	}

	/**
	 * How many MBeans in this Server?
	 *
	 * @return
	 * @throws IOException
	 */
	public int getMBeanCount() throws IOException {
		return mbsc.getMBeanCount();
	}

	/**
	 * List all the JMX Domains for this MBean Server.
	 *
	 * @param mbsc
	 * @return
	 * @throws IOException
	 */
	public String[] getDomains(MBeanServerConnection mbsc) throws IOException {
		String domains[] = mbsc.getDomains();
		Arrays.sort(domains);
		return domains;
	}
}
