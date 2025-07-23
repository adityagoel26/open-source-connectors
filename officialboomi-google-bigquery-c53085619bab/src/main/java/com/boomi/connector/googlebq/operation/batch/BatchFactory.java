// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.batch;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.CollectionUtil;

import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * Provides an iterator that returns a {@link Batch} for the input document. A {@link Batch} consists of documents
 * that can be sent in a single http request.
 *
 * @author Rohan Jain
 */
public class BatchFactory implements Iterable<Batch> {

    private static final String ERROR_BATCH_CREATION = "an error condition was reached during a request batch creation";
    private static final int MAX_CONCURRENT_DOCUMENTS = 100000;
    private static final int MAX_CONCURRENT_BATCHES = 1000;
    private final int _maxCount;
    private final long _maxSizeInBytes;
    private final String _templateSuffix;
    private final Map<String, Batch> _batches = new HashMap<>();

    private final Iterator<ObjectData> _documentIterator;
    private final UpdateRequest _updateRequest;
    private final OperationResponse _operationResponse;

    private ObjectData _previousDocument;
    private int _documentsCount;

    public BatchFactory(UpdateRequest updateRequest, String templateSuffix, int batchCount, long maxSizeInBytes,
            OperationResponse operationResponse) {

        if(maxSizeInBytes < 0 || batchCount < 0 ) {
            throw new ConnectorException("Maximum Batch Size or Maximum Count cannot be less than 0");
        }
        _maxCount = batchCount;
        _maxSizeInBytes = maxSizeInBytes;
        _templateSuffix = templateSuffix;
        _updateRequest = updateRequest;
        _documentIterator = updateRequest.iterator();
        _operationResponse = operationResponse;
    }

    public void markRemainingDocumentsAsFailed( OperationResponse opResponse, Response response){
        Status status = response.getStatus();
        String code = String.valueOf(status.getCode());
        Payload payload = ResponseUtil.toErrorPayload(response, opResponse.getLogger());

        if(_previousDocument != null){
            opResponse.addResult(_previousDocument, OperationStatus.FAILURE, code, status.getDescription(), payload);
        }
        if(_documentIterator.hasNext()){
            opResponse.addCombinedResult(_updateRequest, OperationStatus.FAILURE, code, status.getDescription(),
                    payload);
        }

    }

    @Override
    public Iterator<Batch> iterator() {
        return new Iterator<Batch>() {

            /**
             * @return true if
             * <ol>
             *     <li>previous document is not null, or</li>
             *     <li>there are more documents to be added to a batch, or</li>
             *     <li>there are pending batches to return</li>
             * </ol>
             */
            @Override
            public boolean hasNext() {
                return (_previousDocument != null) || (_documentIterator.hasNext()) ||
                        !CollectionUtil.isEmpty(_batches);
            }

            /**
             * Returns a {@link Batch} which consists of a list of input documents which can be sent in a single
             * request to google big query api.
             * Documents are stored in a new {@link Batch} when {@link Batch#canFit(long)} returns false.
             * There are limits based on number of documents and the total size in bytes of documents
             * which determine the batch size. The Google big query api has limits on number of records in a
             * single request and the size of the request.
             *
             * When {@link Batch#canFit(long)} returns false it means a limit has exceeded and
             * no more documents can be added to the current batch. The current document is stored in a previousDocument
             * variable so that it can be added to the next new batch.
             */
            @Override
            public Batch next() {
                addPreviousDocument();
                Batch batch = _documentIterator.hasNext() ? processInput() : CollectionUtil.getFirst(_batches.values());
                if (batch == null) {
                    throw new NoSuchElementException(ERROR_BATCH_CREATION);
                }
                _documentsCount -= batch.getBatchCount();
                return _batches.remove(batch.getTemplateSuffix());
            }

            /**
             * Adds the previous document to a new {@link Batch}. previous document is assigned when
             * {@link Batch#canFit(long)} returns false which does not add the current document to the
             * {@link Batch} since limit exceeds. This current document is stored as previous document
             * so that it can be added to the next new batch.
             */
            private void addPreviousDocument() {
                if (_previousDocument == null) {
                    return;
                }
                try {
                    Batch batch = addDocument(_previousDocument);
                    //Having a batch completed at this moment means that the document couldn't be added to the batch
                    // and _previousDocument shouldn't be nullified
                    if (!batch.isComplete()) {
                        _previousDocument = null;
                    }
                }
                catch (IOException e) {
                    addApplicationError(_previousDocument, e);
                }

            }

            private boolean isOverflown(Batch batch){
                return batch != null && ((MAX_CONCURRENT_DOCUMENTS < _documentsCount)
                        || (MAX_CONCURRENT_BATCHES < _batches.size()));
            }

            private Batch processInput() {
                Batch batch = null;
                while (!isOverflown(batch) && _documentIterator.hasNext() && (batch == null || !batch.isComplete())) {
                    ObjectData document = _documentIterator.next();
                    try {
                        batch = addDocument(document);
                    }
                    catch (IOException e) {
                        addApplicationError(document, e);
                    }
                }

                return batch;
            }

            private Batch addDocument(ObjectData document) throws IOException {
                String templateSuffix = extractTemplateSuffix(document);
                Batch batch = _batches.get(templateSuffix);
                if (batch == null) {
                    batch = new Batch(templateSuffix, _maxCount, _maxSizeInBytes, _operationResponse);
                    _batches.put(templateSuffix, batch);
                }

                //We want to add a new document if the batch can hold it or force it if the batch is new and  the
                // document is larger than the allowed size to let it fail in the API instead.
                if (batch.getBatchCount() == 0 || batch.canFit(document.getDataSize())) {
                    batch.addDocument(document);
                    _documentsCount++;
                } else {
                    _previousDocument = document;
                    batch.complete();
                }

                return batch;
            }

            private String extractTemplateSuffix(ObjectData data) {
                String dynamicTemplateSuffix = data.getDynamicProperties().get(GoogleBqConstants.PROP_TEMPLATE_SUFFIX);
                return (dynamicTemplateSuffix == null) ? _templateSuffix : dynamicTemplateSuffix;
            }

            private void addApplicationError(ObjectData document, Throwable t) {
                document.getLogger().log(Level.WARNING, t.getMessage(), t);
                _operationResponse.addResult(document, OperationStatus.APPLICATION_ERROR, String.valueOf(Status
                        .CLIENT_ERROR_BAD_REQUEST.getCode()), t.getMessage(), null);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
