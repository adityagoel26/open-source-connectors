//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sftp;

import com.jcraft.jsch.Session;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;

/**
 * A factory for creating Session objects.
 * @author sweta.b.das
 */
public class SessionFactory extends BaseKeyedPoolableObjectFactory<ConnectionProperties, Session> {


	/**
	 * This creates a Session if not already present in the pool.
	 *
	 * @param conProp the con prop
	 * @return the session
	 * @throws Exception the exception
	 */
	@Override
	public Session makeObject(ConnectionProperties conProp) {
		return ManageSession.getSessionWithoutConnectionPooling(conProp);
	}


	/**
	 * This is called when closing the pool objects.
	 *
	 * @param serverDetails the server details
	 * @param session       the session
	 */
	@Override
	public void destroyObject(ConnectionProperties serverDetails, Session session) {
		ManageSession.killSession(session);
	}

}