//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.jcraft.jsch.Session;

/**
 * The Class DHGEX256_1024.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class DHGEX256For1024 extends DHGEX1024 {
	
	/**
	 * Inits the.
	 *
	 * @param session the session
	 * @param vs the v s
	 * @param vc the v c
	 * @param is the i s
	 * @param ic the i c
	 * @throws Exception the exception
	 */
	@Override
	public void init(Session session, byte[] vs, byte[] vc, byte[] is, byte[] ic) throws Exception {
		this.hash = "sha-256";
		super.init(session, vs, vc, is, ic);
	}
}
