// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.listener;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.boomi.connector.sapjco.SAPJcoConnection;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.connector.sapjco.util.SAPUtil;
import com.boomi.util.ObjectUtil;
import com.sap.conn.jco.ext.ServerDataEventListener;
import com.sap.conn.jco.ext.ServerDataProvider;

/**
 * @author a.kumar.samantaray
 *
 */
public class SAPServerDataProvider implements ServerDataProvider {
	private ServerDataEventListener el;
	private ConcurrentHashMap<String, Properties> servers = new ConcurrentHashMap<>();
	public Properties getServerProperties(String serverName) {
		return this.servers.get(serverName);
	}

	public void setServerDataEventListener(ServerDataEventListener el) {
		this.el = el;
	}

	public boolean supportsEvents() {
		return true;
	}

	/**
     * This method adds the Listener host and services details in Properties and registers in server.
     * @param serverName
     * @param conn
     * @param programId
     * @param repoDestName
     * 
     */
	public void registerServer(String serverName, SAPJcoConnection conn, String programId, String repoDestName) {
		String connType = (String) ObjectUtil.defaultIfNull((Object) conn.getConnectionType(),
				(Object) SAPJcoConstants.AHOST);
		Properties props = new Properties();
		props.setProperty("jco.server.connection_count", conn.getListenerConnectionCount().toString());
		if(connType.equals(SAPJcoConstants.AHOST)) {
			props.setProperty("jco.server.gwhost", conn.getListenerGatewayHost());
			props.setProperty("jco.server.gwserv", conn.getListenerGatewayService());
		}else {
			props.setProperty("jco.server.mshost", conn.getServer());
			props.setProperty("jco.server.system_id", conn.getSystemName());
			SAPUtil.setIfNotBlank(props, "jco.server.group", conn.getGroupName());
		}
		props.setProperty("jco.server.progid", programId);
		props.setProperty("jco.server.repository_destination", repoDestName);
		
		if(!conn.getAdditionalProperties().isEmpty()) {
			Properties additionalProperties = conn.getAdditionalProperties();
			SAPUtil.setIfNotBlank(props, ServerDataProvider.JCO_SNC_MODE, additionalProperties.getProperty(ServerDataProvider.JCO_SNC_MODE));
			SAPUtil.setIfNotBlank(props, ServerDataProvider.JCO_SNC_QOP, additionalProperties.getProperty(ServerDataProvider.JCO_SNC_QOP));
			SAPUtil.setIfNotBlank(props, ServerDataProvider.JCO_SNC_MYNAME, additionalProperties.getProperty(ServerDataProvider.JCO_SNC_MYNAME));
			SAPUtil.setIfNotBlank(props, ServerDataProvider.JCO_SNC_LIBRARY, additionalProperties.getProperty(ServerDataProvider.JCO_SNC_LIBRARY));
		}
		this.servers.put(serverName, props);
	}

	/**
     * This method unregisters the server from servers list.
     * @param serverName
     * 
     */
	public void unregisterServer(String serverName) {
		if (this.servers.remove(serverName) != null) {
			this.el.deleted(serverName);
		}
	}

}
