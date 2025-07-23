//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import static com.boomi.connector.sftp.constants.SFTPConstants.PROPERTY_INCLUDE_METADATA;

import java.io.IOException;
import java.io.InputStream;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.actions.RetryableQueryAction;
import com.boomi.connector.sftp.common.FileMetadata;
import com.boomi.connector.sftp.common.GetSFTPFileMetadata;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.results.BaseResult;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * The Class QueryResultBuilder.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class QueryResultBuilder implements ResultBuilder {

	/** The file content. */
	private InputStream fileContent;

	/**
	 * Gets the filecontent.
	 *
	 * @return the filecontent
	 */
	public InputStream getFilecontent() {
		return fileContent;
	}

	/**
	 * Make result.
	 *
	 * @param meta the meta
	 * @param dirFullPsth the dir full psth
	 * @param retryableGetaction the retryable getaction
	 * @return the base result
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public BaseResult makeResult(LsEntry meta, String dirFullPsth, RetryableQueryAction retryableGetaction, PropertyMap propertyMap, OperationResponse operationResponse)
			throws IOException {
		try {
			retryableGetaction.execute();
			PayloadMetadata metadata = operationResponse.createMetadata();
			metadata.setTrackedProperty(SFTPConstants.PROPERTY_FILENAME, meta.getFilename());
			fileContent = (retryableGetaction).getOutputStream().toInputStream();
			String timeStamp = FileMetadata.formatDate(FileMetadata.parseDate(meta.getAttrs().getMTime() * 1000L));
			boolean includeAllMetadata = propertyMap
					.getBooleanProperty(PROPERTY_INCLUDE_METADATA, Boolean.FALSE);
			if (!includeAllMetadata) {
				return new BaseResult(PayloadUtil.toPayload(fileContent, metadata));
			} else {
				GetSFTPFileMetadata getFileMetadata = new GetSFTPFileMetadata(fileContent, meta.getFilename(), timeStamp);
				return new BaseResult(PayloadUtil.toPayload(getFileMetadata.toJson(), metadata));
			}
		}
		
		finally {
			(retryableGetaction).close();
		}

	}

}
