// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.RequestUtil;
import com.boomi.connector.mongodb.constants.BoomiConstants;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.util.DocumentUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.CollectionUtil;
import com.boomi.util.CollectionUtil.Filter;
import com.boomi.util.IOUtil;

/**
 * Implements logic to prepare batches for a given request.
 *
 */
public class BatchDocuments extends InputWrapper {

	/** The memory used. */
	Long memoryUsed = 0L;

	/** The logger. */
	static Logger logger = Logger.getLogger(BatchDocuments.class.getName());

	/** The input doc. */
	Document inputDoc = null;

	/** The remaininglist. */
	List<ObjectData> remaininglist = new ArrayList<>();

	/** The include size exceeded payload. */
	boolean includeSizeExceededPayload = true;

	/** The opr response. */
	OperationResponse oprResponse = null;

	/** The config. */
	AtomConfig config = null;

	/** The request. */
	Iterable<ObjectData> request = null;

	/** The batch size. */
	int batchSize = 0;

	/** The charset. */
	Charset charset = null;

	/** Iterator that iterates over batches. */
	Iterator<List<ObjectData>> requestBatchInputItr = new ArrayList<List<ObjectData>>().iterator();

	/** The total items in curr batch. */
	int totalItemsInCurrBatch = 0;

	/**
	 * Gets the charset.
	 *
	 * @return the charset
	 */
	public Charset getCharset() {
		return charset;
	}

	/**
	 * Sets the charset.
	 *
	 * @param charset the new charset
	 */
	public void setCharset(Charset charset) {
		this.charset = charset;
	}

	/**
	 * Instantiates a new batch documents object.
	 *
	 * @param request                    the request
	 * @param batchSize                  the batch size
	 * @param atomConfig                 the atom configuration
	 * @param charset                    the charset to read the request input
	 *                                   stream as string. If no charset is
	 *                                   provided, UTF-8 charset is used by default
	 * @param response                   the response
	 * @param includeSizeExceededPayload the include size exceeded payload
	 */
	public BatchDocuments(Iterable<ObjectData> request, int batchSize, AtomConfig atomConfig, Charset charset,
			OperationResponse response, boolean includeSizeExceededPayload) {
		Iterable<ObjectData> reqFilteredBasedOnSize = CollectionUtil.filter(request,
				new ObjectDataSizeFilter(response));
		oprResponse = response;
		config = atomConfig;
		this.batchSize = batchSize;
		this.requestBatchInputItr = RequestUtil.pageIterable(reqFilteredBasedOnSize, batchSize, atomConfig).iterator();
		if (null == charset) {
			charset = StandardCharsets.UTF_8;
		} else {
			this.charset = charset;
		}
		this.includeSizeExceededPayload = includeSizeExceededPayload;
	}

	/**
	 * Checks if next input batch is available.
	 *
	 * @return true, if successful
	 */
	public boolean hasNext() {
		return (!remaininglist.isEmpty() || requestBatchInputItr.hasNext());
	}

	/**
	 * Process input @link ObjectData to Mongo Document.
	 *
	 * @param input       the input
	 * @param charset     the charset
	 * @param recordIndex the record index
	 * @return the tracked data wrapper
	 */
	public TrackedDataWrapper processObjectData(ObjectData input, Charset charset, int recordIndex) {
		Document doc = null;
		String jsonStr = null;
		Exception ex = null;
		InputStream inputStream = null;
		TrackedDataWrapper data = null;
		try {
			inputStream = input.getData();
			totalItemsInCurrBatch = getTotalItemsInCurrBatch() + 1;
			setTotalItemsInCurrBatch(totalItemsInCurrBatch);
			jsonStr = DocumentUtil.inputStreamToString(inputStream, charset);
			doc = Document.parse(jsonStr);
		} catch (JsonParseException | IOException e) {
			ex = e;
			input.getLogger().log(Level.SEVERE,
					new StringBuffer("Error while parsing JSON record to Document for inputRecord: ")
							.append(recordIndex).toString());
		} finally {
			IOUtil.closeQuietly(inputStream);
			data = new TrackedDataWrapper(input, doc);
			if (null != ex) {
				String exceptionMessage = "Error while parsing JSON record to Document for inputRecord: "+recordIndex;
				data.setErrorDetails(null, exceptionMessage); //errormessage set as null initially now updated set message value
				getAppErrorRecords().add(data);
			}
		}
		return data;
	}

	/**
	 * Prepares the next batch of input data to be processed in the operation by
	 * parsing the input . It checks the size of each batch and do not the batch
	 * more than a total of 1MB into the memory. When the batch size reaches 1MB
	 * then the remaining items are added to the remaining list.
	 *
	 * @return the list
	 */
	public List<TrackedDataWrapper> next() {
		List<TrackedDataWrapper> list = new ArrayList<>();
		int currentBatchSize = 0;
		TrackedDataWrapper data = null;
		long memoryInput = 0L;
		long memUsed = 0L;

		if (!remaininglist.isEmpty()) {
			Iterator<ObjectData> remaininglistItr = getRemaininglist().iterator();
			while (remaininglistItr.hasNext()) {
				ObjectData remObjData = remaininglistItr.next();
				updateMemUsed(remObjData);
				if (getMemoryUsed() > BoomiConstants.MAX_SIZE) {
					break;
				}
				remaininglistItr.remove();
				data = processObjectData(remObjData, getCharset(), currentBatchSize + 1);
				addDataToList(list, data);

			}
		} else if (getMemoryUsed() <= BoomiConstants.MAX_SIZE) {
			processBatchInputData(list, currentBatchSize, memoryInput, memUsed);
		}
		String temp = new StringBuffer("Total number of documents parsed in ")
				.append("batch :").append(batchCounter + 1).append(" are ").append(totalItemsInCurrBatch).toString();
		oprResponse.getLogger().log(Level.INFO, temp);
		batchCounter++;
		setMemoryUsed(0L);
		setTotalItemsInCurrBatch(0);
		return list;
	}

	private void processBatchInputData(List<TrackedDataWrapper> list, int currentBatchSize, long memoryInput, long memUsed) {
		List<ObjectData> nextBatch;
		ObjectData input;
		long memoryAvailable;
		TrackedDataWrapper data;
		nextBatch = requestBatchInputItr.next();
		for (; currentBatchSize < nextBatch.size(); currentBatchSize++) {
			input = nextBatch.get(currentBatchSize);
			try {
				memoryInput = input.getDataSize();
			} catch (IOException e) {
				logger.log(Level.SEVERE, "Unable to fetch data size", e);
			}
			memUsed = memUsed + memoryInput;
			memoryAvailable = BoomiConstants.MAX_SIZE - memUsed;
			if (memoryAvailable >= 0) {
				data = processObjectData(input, getCharset(), currentBatchSize + 1);
				addDataToList(list, data);
			} else {
				getRemaininglist().add(input);
			}
		}
	}

	private static void addDataToList(List<TrackedDataWrapper> list, TrackedDataWrapper data) {
		if (null != data && null != data.getDoc()) {
			list.add(data);
		}
	}

	/**
	 * Filter for {@link ObjectData} instances that exceed the allowed size limit.
	 */
	private class ObjectDataSizeFilter implements Filter<ObjectData> {

		/** The op response. */
		private final OperationResponse opResponse;

		/** The filtered recs counter. */
		private int filteredRecsCounter = 0;

		/**
		 * Instantiates a new object data size filter.
		 *
		 * @param response the response
		 */
		private ObjectDataSizeFilter(OperationResponse response) {
			logger.log(Level.INFO, "Max allowed size: {0} bytes", BoomiConstants.MAX_SIZE);
			opResponse = response;
		}

		/**
		 * Accepts object data instances whose size does not exceed the limit. If the
		 * size is exceeded an application error result will be added for that input.
		 *
		 * @param data the data
		 * @return true if the input does not exceed the size limit, false otherwise
		 */
		@Override
		public boolean accept(ObjectData data) {
			try(Payload payload = getPayload(data);) {
				if (isAllowedSize(data)) {
					return true;
				}
				++filteredRecsCounter;
				logger.log(Level.SEVERE, "Input exceed maxSize: {0} Bytes | filteredRecsCounter-{1}",
						new Object[] { BoomiConstants.MAX_SIZE, filteredRecsCounter });
				opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, BoomiConstants.DEFAULT_STATUS_CODE,
						BoomiConstants.DEFAULT_STATUS_MESSAGE, payload);
			} catch (IOException e) {
				throw new ConnectorException(e);
			} 
			return false;
		}

		/**
		 * Attempts to convert the provided object data to a payload.
		 * 
		 * @param data the input data
		 * @return payload containing the input data if
		 *         {@link SizeLimitedUpdateOperation #includeSizeExceededPayload()}
		 *         returns true, null otherwise
		 */
		private Payload getPayload(ObjectData data) {
			if (includeSizeExceededPayload) {
				InputStream input = data.getData();
				try {
					return PayloadUtil.toPayload(input);
				} finally {
					IOUtil.closeQuietly(input);
				}
			}
			return null;
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
			Long inputSize = -1L;
			boolean isAllowedSize = false;
			try {
				inputSize = data.getDataSize();
			} catch (IOException e) {
				String message = "unknown size: " + data.getUniqueId();
				data.getLogger().log(Level.WARNING, message, e);
			}
			if (inputSize != -1L) {
				isAllowedSize = inputSize <= BoomiConstants.MAX_SIZE;
			}
			return isAllowedSize;
		}
	}

	/**
	 * Gets the input doc.
	 *
	 * @return the input doc
	 */
	public Document getInputDoc() {
		return inputDoc;
	}

	/**
	 * Sets the input document.
	 *
	 * @param inputDoc the new input doc
	 */
	public void setInputDoc(Document inputDoc) {
		if (inputDoc.isEmpty()) {
			logger.log(Level.SEVERE, "Parsed doc is empty!");
		} else if (null != this.inputDoc) {
			this.inputDoc.clear();

		} else {
			this.inputDoc = new Document();
		}
		if (!inputDoc.containsKey(MongoDBConstants.ID_FIELD_NAME)) {
			this.inputDoc.append(MongoDBConstants.ID_FIELD_NAME, ObjectId.get());
		}
		for (Entry<String, Object> entry : inputDoc.entrySet()) {
			this.inputDoc.append(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Gets the list from iterator.
	 *
	 * @param iterator the iterator
	 * @return the list from iterator
	 */
	public List<ObjectData> getListFromIterator(Iterator<ObjectData> iterator) {
		List<ObjectData> list = new ArrayList<>();
		while (iterator.hasNext()) {
			list.add(iterator.next());
		}
		return list;
	}

	/**
	 * Gets the memory used.
	 *
	 * @return the memory used
	 */
	public Long getMemoryUsed() {
		return memoryUsed;
	}

	/**
	 * Sets the memory used.
	 *
	 * @param memoryUsedVal the new memory used
	 */
	public void setMemoryUsed(Long memoryUsedVal) {
		memoryUsed = memoryUsedVal;
	}

	/**
	 * Updates the memory used in a batch.
	 *
	 * @param objData the obj data
	 * @return the long
	 */
	public Long updateMemUsed(ObjectData objData) {
		BatchDocuments batchDocuments = new BatchDocuments(request, batchSize, config, charset, oprResponse,
				includeSizeExceededPayload);
		BatchDocuments.ObjectDataSizeFilter objectDataSizeFilter = batchDocuments.new ObjectDataSizeFilter(oprResponse);
		Payload payload = null;
		try {
			setMemoryUsed(getMemoryUsed() + objData.getDataSize());
		} catch (IOException ex) {
			logger.log(Level.SEVERE, "Unable to fetch data size");
			payload = objectDataSizeFilter.getPayload(objData);
			oprResponse.addResult(objData, OperationStatus.APPLICATION_ERROR, BoomiConstants.DEFAULT_STATUS_CODE,
					BoomiConstants.DEFAULT_STATUS_MESSAGE, payload);
		} finally {
			IOUtil.closeQuietly(payload);
		}
		return getMemoryUsed();
	}

	/**
	 * Gets the total items in current batch.
	 *
	 * @return the total items in current batch
	 */
	public int getTotalItemsInCurrBatch() {
		return totalItemsInCurrBatch;
	}

	/**
	 * Sets the total items in current batch.
	 *
	 * @param totalItemsInCurrBatch the new total items in curr batch
	 */
	public void setTotalItemsInCurrBatch(int totalItemsInCurrBatch) {
		this.totalItemsInCurrBatch = totalItemsInCurrBatch;
	}

	/**
	 * Gets the remaining list of input for processing.
	 *
	 * @return the remaininglist
	 */
	public List<ObjectData> getRemaininglist() {
		return remaininglist;
	}

	/**
	 * Sets the remaininglist.
	 *
	 * @param remaininglist the new remaininglist
	 */
	public void setRemaininglist(List<ObjectData> remaininglist) {
		this.remaininglist = remaininglist;
	}

}
