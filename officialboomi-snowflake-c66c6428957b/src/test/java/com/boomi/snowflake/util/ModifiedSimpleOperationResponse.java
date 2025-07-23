// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.boomi.connector.api.ExtendedPayload;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.StreamUtil;

public class ModifiedSimpleOperationResponse implements OperationResponse {

	private static final Logger logger = Logger.getLogger("com.boomi.connector.testutil");
	private final Map<SimpleTrackedData, Status> _outstanding = new HashMap<SimpleTrackedData, Status>();
	private final List<SimpleOperationResult> _results = new ArrayList<SimpleOperationResult>();
	private List<byte[]> _pendingPayloads;
	private List<SimplePayloadMetadata> _payloadMetadatas = new ArrayList<SimplePayloadMetadata>();

	public void addResult(TrackedData input, OperationStatus status, String statusCode, String statusMessage,
			Payload payload) {
		this.updateStatus(Collections.singletonList(input), status, statusCode, statusMessage, (Throwable) null);
		this.finishInput(Collections.singletonList(input), payload, (List<byte[]>) null);
		if (payload != null && payload instanceof ExtendedPayload
				&& ((ExtendedPayload) payload).getMetadata() instanceof SimplePayloadMetadata) {
			this._payloadMetadatas.add((SimplePayloadMetadata) ((ExtendedPayload) payload).getMetadata());
		}

	}

	public void addCombinedResult(Iterable<? extends TrackedData> inputs, OperationStatus status, String statusCode,
			String statusMessage, Payload payload) {
		this.updateStatus(inputs, status, statusCode, statusMessage, (Throwable) null);
		this.finishInput(inputs, payload, (List<byte[]>) null);
		if (payload != null && payload instanceof ExtendedPayload
				&& ((ExtendedPayload) payload).getMetadata() instanceof SimplePayloadMetadata) {

			this._payloadMetadatas.add((SimplePayloadMetadata) ((ExtendedPayload) payload).getMetadata());
		}

	}

	public void addPartialResult(TrackedData input, OperationStatus status, String statusCode, String statusMessage,
			Payload payload) {
		this.updateStatus(Collections.singletonList(input), status, statusCode, statusMessage, (Throwable) null);

		if (payload != null) {
			if (this._pendingPayloads == null) {
				this._pendingPayloads = new ArrayList<byte[]>();
			}
			this._pendingPayloads.add(toResult(payload));
		}
		if (payload != null && payload instanceof ExtendedPayload
				&& ((ExtendedPayload) payload).getMetadata() instanceof SimplePayloadMetadata) {

			this._payloadMetadatas.add((SimplePayloadMetadata) ((ExtendedPayload) payload).getMetadata());
		}

	}

	public void finishPartialResult(TrackedData input) {
		List<byte[]> pendingPayloads = this._pendingPayloads;
		this._pendingPayloads = null;
		this.finishInput(Collections.singletonList(input), (Payload) null, pendingPayloads);
	}

	public void addPartialResult(Iterable<? extends TrackedData> inputs, OperationStatus status, String statusCode,
			String statusMessage, Payload payload) {
		this.updateStatus(inputs, status, statusCode, statusMessage, (Throwable) null);

		if (payload != null) {
			if (this._pendingPayloads == null) {
				this._pendingPayloads = new ArrayList<byte[]>();
			}
			this._pendingPayloads.add(toResult(payload));
		}
		if (payload != null && payload instanceof ExtendedPayload
				&& ((ExtendedPayload) payload).getMetadata() instanceof SimplePayloadMetadata) {

			this._payloadMetadatas.add((SimplePayloadMetadata) ((ExtendedPayload) payload).getMetadata());
		}

	}

	public void finishPartialResult(Iterable<? extends TrackedData> inputs) {
		List<byte[]> pendingPayloads = this._pendingPayloads;
		this._pendingPayloads = null;
		this.finishInput(inputs, (Payload) null, pendingPayloads);
	}

	public void addEmptyResult(TrackedData input, OperationStatus status, String statusCode, String statusMessage) {
		this.updateStatus(Collections.singletonList(input), status, statusCode, statusMessage, (Throwable) null);
		this.finishInput(Collections.singletonList(input), (Payload) null, (List<byte[]>) null);
	}

	public void addErrorResult(TrackedData input, OperationStatus status, String statusCode, String statusMessage,
			Throwable t) {
		this.updateStatus(Collections.singletonList(input), status, statusCode, statusMessage, t);
		this.finishInput(Collections.singletonList(input), (Payload) null, (List<byte[]>) null);
	}

	public PayloadMetadata createMetadata() {
		return new SimplePayloadMetadata();
	}

	public Logger getLogger() {
		return logger;
	}

	private void updateStatus(Iterable<? extends TrackedData> inputs, OperationStatus opStatus, String statusCode,
			String statusMessage, Throwable thrown) {
		if (inputs == null) {
			throw new AssertionError("missing input collection (is null)");

		} else {
			Iterator<? extends TrackedData> i$ = inputs.iterator();
			while (i$.hasNext()) {
				TrackedData input = (TrackedData) i$.next();
				if (input == null) {
					throw new AssertionError("missing input (is null)");
				}

				SimpleTrackedData data = (SimpleTrackedData) input;
				Status status = (Status) this._outstanding.get(data);
				if (status == null) {
					throw new AssertionError("Input " + data + " was not outstanding (already marked as finished)");
				}

				updateStatusFields(status, opStatus, statusCode, statusMessage, thrown);
			}
		}
	}

	private static void updateStatusFields(Status status, OperationStatus opStatus, String statusCode,
			String statusMessage, Throwable thrown) {
		if (status.operationStatus == null
				|| status.operationStatus == OperationStatus.SUCCESS && opStatus == OperationStatus.APPLICATION_ERROR
				|| status.operationStatus != OperationStatus.FAILURE && opStatus == OperationStatus.FAILURE) {

			status.operationStatus = opStatus;
			status.statusCode = statusCode;
			status.statusMessage = statusMessage;
			status.thrown = thrown;
		}

	}

	private void finishInput(Iterable<? extends TrackedData> inputs, Payload payload, List<byte[]> payloads) {
		Status resultStatus = new Status(null);

		Iterator<? extends TrackedData> i$ = inputs.iterator();
		while (i$.hasNext()) {
			TrackedData input = (TrackedData) i$.next();
			SimpleTrackedData data = (SimpleTrackedData) input;
			Status status = (Status) this._outstanding.remove(data);
			updateStatusFields(resultStatus, status.operationStatus, status.statusCode, status.statusMessage, (Throwable) null);
		}

		if (payloads == null) {
			payloads = new ArrayList<byte[]>();
		}
		if (payload != null) {
			((List<byte[]>) payloads).add(toResult(payload));
		}

		this._results.add(new SimpleOperationResult(resultStatus.operationStatus, resultStatus.statusCode,
				resultStatus.statusMessage, (List<byte[]>) payloads, resultStatus.thrown, this._payloadMetadatas));

	}

	void addTrackedData(SimpleTrackedData data) {
		this._outstanding.put(data, new Status(null));
	}

	void close(Throwable failure) {
		if (!this._outstanding.isEmpty()) {
			if (failure == null) {
				failure = new IllegalStateException("input was lost during operation execution");
			}
			this.updateStatus(this._outstanding.keySet(), OperationStatus.FAILURE, (String) null,
					((Throwable) failure).getMessage(), (Throwable) failure);
			this.finishInput(this._outstanding.keySet(), (Payload) null, (List<byte[]>) null);
		}
	}

	public List<SimpleOperationResult> getResults() {
		return this._results;
	}

	public void addOperationResult(SimpleOperationResult result) {
		_results.add(result);
	}

	private static byte[] toResult(Payload payload) {
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream(1024);
			InputStream in = payload.readFrom();
			if (in != null) {
				StreamUtil.copy(in, bout);
			} else {
				payload.writeTo(bout);
			}

			return bout.toByteArray();
		} catch (Exception var3) {
			throw new AssertionError(var3);
		}
	}

	public List<SimplePayloadMetadata> getPayloadMetadatas() {
		return this._payloadMetadatas;
	}

}
