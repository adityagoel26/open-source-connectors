// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseConnection;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.ByteUnit;
import com.boomi.util.CollectionUtil;
import com.boomi.util.CollectionUtil.Filter;


/**
 * @author kishore.pulluru
 *
 */
public abstract class SizeLimitedUpdateOperation extends BaseUpdateOperation {

	private static final String DEFAULT_STATUS_MESSAGE = "max size exceeded";
	private static final String DEFAULT_STATUS_CODE = "413";
	private static final long MAX_SIZE = ByteUnit.MB.getByteUnitSize();

	/**
	 * Creates a new instance using the provided connection
	 */
	protected SizeLimitedUpdateOperation(BaseConnection connection) {
		super(connection);
	}

	/**
	 * Creates a new instance using the provide operation context
	 */
	protected SizeLimitedUpdateOperation(OperationContext context) {
		super(context);
	}

	/**
	 * Creates a filtered {@link UpdateRequest} that does not include any documents
	 * that exceed the size limit. The filtered request is passed to
	 * {@link #executeSizeLimitedUpdate(UpdateRequest, OperationResponse)}
	 * implementation.
	 */
	@Override
	protected final void executeUpdate(final UpdateRequest request, final OperationResponse response) {
		executeSizeLimitedUpdate(new UpdateRequest() {
			@Override
			public Iterator<ObjectData> iterator() {
				return CollectionUtil.filter(request, new ObjectDataSizeFilter(response)).iterator();
			}
		}, response);
	}

	/**
	 * Execute the implementation update logic
	 * 
	 * @param request  the filtered request
	 * @param response the operation response
	 */
	protected abstract void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response);

	/**
	 * Returns the status code for excluded. By default, returns
	 * {@link #DEFAULT_STATUS_CODE}.
	 * 
	 * @return the status code
	 */
	protected String getSizeExceededStatusCode() {
		return DEFAULT_STATUS_CODE;
	}

	/**
	 * Returns the status message for excluded inputs. By default, returns
	 * {@link #DEFAULT_STATUS_MESSAGE}.
	 * 
	 * @return the status message
	 */
	protected String getSizeExceededMessage() {
		return DEFAULT_STATUS_MESSAGE;
	}

	/**
	 * Indicates if the input document should be echoed as the application error
	 * payload. By default, returns false. When false, a null payload is used. The
	 * payload will contain metadata either way.
	 * 
	 * @return true if the payload should be included, false otherwise
	 */
	protected boolean includeSizeExceededPayload() {
		return false;
	}

	/**
	 * Filter for {@link ObjectData} instances that exceed the allowed size limit.
	 */
	private class ObjectDataSizeFilter implements Filter<ObjectData> {

		private final OperationResponse response;

		private ObjectDataSizeFilter(OperationResponse response) {
			this.response = response;
		}

		/**
		 * Accepts object data instances whose size does not exceed the limit. If the
		 * size is exceeded an application error result will be added for that input.
		 * 
		 * @return true if the input does not exceed the size limit, false otherwise
		 */
		@Override
		public boolean accept(ObjectData data) {
			if (isAllowedSize(data)) {
				return true;
			}
			response.addResult(data, OperationStatus.APPLICATION_ERROR, getSizeExceededStatusCode(), getSizeExceededMessage(), null);
			return false;
		}

		/**
		 * Determines if the size of the input data is allowed. If the size cannot be
		 * determined, it's not allowed.
		 * 
		 * @param data the input data
		 * @return false if the input data size exceeds the limit or cannot be
		 *         determined, true otherwise
		 */
		private boolean isAllowedSize(ObjectData data) {
			try {
				return (data.getDataSize() <= MAX_SIZE);
			} catch (IOException e) {
				data.getLogger().log(Level.WARNING, "unknown size: " + data.getUniqueId(), e);
				return false;
			}
		}
	}

}
