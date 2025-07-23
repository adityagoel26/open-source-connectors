// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp;

import static org.junit.Assert.*;

import org.junit.Test;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.sftp.operations.SFTPCreateOperation;
import com.boomi.connector.sftp.operations.SFTPDeleteOperation;
import com.boomi.connector.sftp.operations.SFTPGetOperation;
import com.boomi.connector.sftp.operations.SFTPQueryOperation;
import com.boomi.connector.sftp.operations.SFTPUpsertOperation;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.connector.testutil.SimpleOperationContext;

public class SFTPConnectorTest {

	@Test
	public void testCreateBrowser() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.CREATE, null, null);
		new SFTPConnection(context);
		SFTPConnector connector= new SFTPConnector();
		assertTrue(connector.createBrowser(context) instanceof SFTPBrowser);

	}

	@Test
	public void testCreateCreateOperationOperationContext() {
		new SimpleBrowseContext(null, null, OperationType.CREATE, null, null);
		SimpleOperationContext opContext= new SimpleOperationContext(null, null, OperationType.CREATE, null, null, null, null);
		SFTPConnector connector= new SFTPConnector();
		assertTrue(connector.createCreateOperation(opContext) instanceof SFTPCreateOperation);
	}

	@Test
	public void testCreateGetOperationOperationContext() {
		new SimpleBrowseContext(null, null, OperationType.GET, null, null);
		SimpleOperationContext opContext= new SimpleOperationContext(null, null, OperationType.GET, null, null, null, null);
		SFTPConnector connector= new SFTPConnector();
		assertTrue(connector.createGetOperation(opContext) instanceof SFTPGetOperation);
	}

	@Test
	public void testCreateQueryOperationOperationContext() {
		new SimpleBrowseContext(null, null, OperationType.QUERY, null, null);
		SimpleOperationContext opContext= new SimpleOperationContext(null, null, OperationType.QUERY, null, null, null, null);
		SFTPConnector connector= new SFTPConnector();
		assertTrue(connector.createQueryOperation(opContext) instanceof SFTPQueryOperation);
	}

	@Test
	public void testCreateDeleteOperationOperationContext() {
		new SimpleBrowseContext(null, null, OperationType.DELETE, null, null);
		SimpleOperationContext opContext= new SimpleOperationContext(null, null, OperationType.DELETE, null, null, null, null);
		SFTPConnector connector= new SFTPConnector();
		assertTrue(connector.createDeleteOperation(opContext) instanceof SFTPDeleteOperation);
	}

	@Test
	public void testCreateUpsertOperationOperationContext() {
		new SimpleBrowseContext(null, null, OperationType.UPSERT, null, null);
		SimpleOperationContext opContext= new SimpleOperationContext(null, null, OperationType.UPSERT, null, null, null, null);
		SFTPConnector connector= new SFTPConnector();
		assertTrue(connector.createUpsertOperation(opContext) instanceof SFTPUpsertOperation);
	}

}
