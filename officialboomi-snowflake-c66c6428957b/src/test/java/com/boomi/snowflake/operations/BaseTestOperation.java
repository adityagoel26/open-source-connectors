// Copyright (c) 2021 Boomi, Inc.
package com.boomi.snowflake.operations;

import com.boomi.connector.api.OperationRequest;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.util.BaseOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.SnowflakeContextIT;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * The Class BaseOperationTest.
 *
 * @author Vanangudi,S
 */
public class BaseTestOperation {
	/** The Constant TIME_OUT. */
	private static final int TIME_OUT = 1000;
	/** The Constant CSV_SPECIAL_STR. */
	private static final String CSV_SPECIAL_STR = "6846,special,20200224 040000.000";
	/** The SnowflakeContextIT object */
	protected SnowflakeContextIT testContext;
	/** The results List of SimpleOperationResult. */
	protected List<SimpleOperationResult> results;
	/** The batchedCount. */
	private long batchedCount = 1;

	/**
	 * Checks if the string is in valid JSON format.
	 * @param test
	 * 			input sting
	 * @return boolean
	 */
	private boolean isJSONValid(String test) {
		if (test == null || test.length() == 0 || test.length() > (1 << 10))
			return true;
		try {
			new JSONObject(test);
		} catch (JSONException ex) {
			// edited, to include @Arthur's comment
			// e.g. in case JSONArray is valid as well...
			try {
				new JSONArray(test);
			} catch (JSONException ex1) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks if the expected operation result matches the actual result
	 * @param status
	 * 			Operation result status
	 * @param payloadsCount
	 * 			Number of payloads in the result
	 * @param outputDocs
	 * 			Output documents
	 */
	protected void assertOperation(OperationStatus status, int payloadsCount, int outputDocs) {
		assertEquals(outputDocs, results.size());
		for (SimpleOperationResult res : results) {
			assertEquals(payloadsCount, res.getPayloads().size());
			if (status == OperationStatus.SUCCESS) {
				for (byte[] lst : res.getPayloads()) {
					assertEquals(true, isJSONValid(new String(lst)));
				}
			}

			assertEquals(status, res.getStatus());
		}
	}
	
	/**
	 * Checks if the expected operation result matches the actual result 
	 * when batch size is greater than 1
	 * @param status
	 * 			Operation result status
	 * @param outputDocs
	 * 			Output documents
	 * @param batchSize
	 * 			Batch Size
	 */
	protected void assertBatchOperation(OperationStatus status, int outputDocs, long batchSize) {
		assertEquals(outputDocs, results.size());
		Iterator<SimpleOperationResult> iterator = results.iterator();
		while (iterator.hasNext()) {
			SimpleOperationResult res = iterator.next();
			if(batchedCount < batchSize && iterator.hasNext()) {
				batchedCount ++;
				assertEquals(0,res.getPayloads().size());
			} else {
				batchedCount = 1;
				assertEquals(1,res.getPayloads().size());
			}
			if (status == OperationStatus.SUCCESS) {
				for (byte[] lst : res.getPayloads()) {
					assertTrue(isJSONValid(new String(lst)));
				}
			}
			assertEquals(status, res.getStatus());
		}
	}

	/**
	 * Checks if the expected operation result matches the actual result 
	 * for CSV out with small tables
	 */
	protected void assertOperationCSVOutputForSmallTable() {
		String output = setupCsvOutputBytes();
		for (SimpleOperationResult res : results) {
			for (byte[] lst : res.getPayloads()) {
				assertArrayEquals(output.getBytes(), lst);
			}
		}
	}

	protected String setupCsvOutputBytes() {
		return "\\\\N,\\\\N,5131321007 052701.000"
				+ "\n"
				+ "\\\\N,\\\\N,20210211 200444.000"
				+ "\n"
				+ "3,2,20210211 200444.000"
				+ "\n"
				+ "3,2,20210211 200444.000"
				+ "\n"
				+ CSV_SPECIAL_STR
				+ "\n"
				+ CSV_SPECIAL_STR
				+ "\n"
				+ CSV_SPECIAL_STR
				+ "\n";
	}

	/**
	 * Checks if the expected operation result matches the actual result 
	 * when connection is closed
	 * @param snfCon
	 * 			Snowflake connection object
	 **/
	protected void assertConnectionIsClosed(SnowflakeConnection snfCon) {
		try {
			if (snfCon.getJDBCConnection() != null)
				assertFalse(snfCon.getJDBCConnection().isValid(TIME_OUT));
		} catch (SQLException e) {
			// do nothing as TIME_OUT is always positive
		}
	}

	/**
	 * Mocks the response for Snowflake operations.
	 *
	 * @param docCount     the document count expected
	 * @param payloadCount the payload size expected
	 * @param status       the operation status
	 * @param invocation   the mock invocation object
	 * @return Void
	 */
	protected Void mockResponse(int docCount, int payloadCount, OperationStatus status, InvocationOnMock invocation) {
		ModifiedSimpleOperationResponse res = invocation.getArgument(1);
		List<byte[]> payloads = new ArrayList<>();
		for (int i = 0; i < payloadCount; i++) {
			payloads.add(new byte[0]);
		}
		for (int i = 0; i < docCount; i++) {
			res.addOperationResult(new SimpleOperationResult(status, "", payloads));
		}
		return null;
	}

	/**
	 * Runs the specified Snowflake operation.
	 *
	 * @param op           the operation
	 * @param request      the request object
	 * @param response     the response object
	 * @param docCount     the document count
	 * @param payloadCount the payload count
	 * @param status       the operation status
	 * @param <T>          the Snowflake operation type
	 */
	@SuppressWarnings("unchecked")
	protected <T extends BaseOperation> void runSnowflakeOperation(T op, OperationRequest request,
			ModifiedSimpleOperationResponse response, int docCount, int payloadCount, OperationStatus status) {

		Mockito.doAnswer((Answer<Void>) inv -> mockResponse(docCount, payloadCount, status, inv))
			   .when(op)
			   .execute(request, response);
		when(op.getConnection()).thenReturn(new SnowflakeConnection(testContext));
		op.execute(request, response);
		results = response.getResults();
	}
}
