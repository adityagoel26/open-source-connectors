// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp;

import org.junit.Test;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.testutil.SimpleBrowseContext;
import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SFTPBrowserTest {

	@Test
	public void testgetObjectDefinitions_CREATE() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.CREATE, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test
	public void testgetObjectDefinitions_GET() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test
	public void testgetObjectDefinitions_QUERY() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.QUERY, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test
	public void testgetObjectDefinitions_UPSERT() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.UPSERT, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test
	public void testgetObjectDefinitions_DELETE() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.DELETE, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test
	public void testgetObjectDefinitions_LIST() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.QUERY, "LIST", null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);
		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testgetObjectDefinitions_UNSUPPORTED_TYPE() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.EXECUTE, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);
		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test
	public void testGetConnection() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.DELETE, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);
		assertEquals(conn, sftpbrowser.getConnection());
	}

	@Test
	public void testgetObjectDefinitions_CREATE_with_All_metadata() {
		Map<String, Object> opProperty = new HashMap<>();
		opProperty.put(SFTPConstants.PROPERTY_INCLUDE_METADATA, Boolean.TRUE);
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.CREATE, null, opProperty);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectDefinitions("abc", Arrays.asList(INPUT, OUTPUT));
	}

	@Test
	public void testgetObjectTypes_CREATE() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.CREATE, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectTypes();
	}

	@Test
	public void testgetObjectTypes_QUERY() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.QUERY, "LIST", null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectTypes();
	}

	@Test
	public void testgetObjectTypes_GET() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectTypes();
	}

	@Test
	public void testgetObjectTypes_DELETE() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.DELETE, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);

		sftpbrowser.getObjectTypes();
	}

	@Test
	public void testgetObjectTypes_UPSERT() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.UPSERT, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);
		sftpbrowser.getObjectTypes();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testgetObjectTypes_Unsupported_operation() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.EXECUTE, null, null);
		SFTPConnection conn = new SFTPConnection(context);
		SFTPBrowser sftpbrowser = new SFTPBrowser(conn);
		sftpbrowser.getObjectTypes();
	}
}
