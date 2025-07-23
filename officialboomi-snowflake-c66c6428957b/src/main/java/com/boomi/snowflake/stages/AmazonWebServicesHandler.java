// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.stages;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.util.IOUtil;
import com.boomi.util.MeteredOutputStream;
import com.boomi.util.TempOutputStream;

import net.snowflake.client.jdbc.internal.amazonaws.AmazonClientException;
import net.snowflake.client.jdbc.internal.amazonaws.auth.AWSCredentials;
import net.snowflake.client.jdbc.internal.amazonaws.auth.AWSStaticCredentialsProvider;
import net.snowflake.client.jdbc.internal.amazonaws.auth.BasicAWSCredentials;
import net.snowflake.client.jdbc.internal.amazonaws.regions.Regions;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3ClientBuilder;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectListing;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.S3ObjectSummary;

public class AmazonWebServicesHandler implements StageHandler {
	private static final String SQL_COMMAND_CREDENTIALS = "CREDENTIALS";
	private static final String SQL_COMMAND_AWS_ACCESS_KEY = "AWS_KEY_ID";
	private static final String SQL_COMMAND_AWS_SECRET = "AWS_SECRET_KEY";
	private static final String SQL_COMMAND_ENCRYPTION = "ENCRYPTION = (TYPE = 'AWS_SSE_S3') ";
	private String _bucketName, _accessKey, _secret;
	private AmazonS3 _s3Client;
	private int _uploadCount;
	private MeteredOutputStream	meteredOutputStream = null;
	private TempOutputStream tempOutputStream = null;
	private GZIPOutputStream gzipStream = null;
	
	/**
	 * @param bucketName AWS bucket name
	 * @param accessKey  AWS access key
	 * @param secret  AWS secret key
	 * @param region     Bucket region
	 */
	public AmazonWebServicesHandler(String bucketName, String accessKey, String secret,
			String region) {
		_uploadCount = 0;
		_bucketName = bucketName;
		_accessKey = accessKey;
		_secret = secret;
		AWSCredentials awsCreds = new BasicAWSCredentials(_accessKey, _secret);
		_s3Client = AmazonS3ClientBuilder.standard().withRegion(region)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
	}

	/**
	 * Retrieves the AWS access key
	 * 
	 * @return access key of the AWS user
	 */
	public String getAccessKey() {
		return _accessKey;
	}

	/**
	 * Retrieves the AWS secret
	 * 
	 * @return secret key of the AWS user
	 */
	public String getSecret() {
		return _secret;
	}

	/**
	 * Retrieves the key names of all objects with a prefix of <code>prefix</code> on the bucket
	 * 
	 * @param prefix Prefix of the target objects
	 * @return list of key names of objects found
	 */
	@Override
	public ArrayList<String> getListObjects(String prefix){
		ArrayList<String> ret = new ArrayList<String>();
		try {
			ObjectListing res = _s3Client.listObjects(_bucketName, prefix);
			for (S3ObjectSummary cur : res.getObjectSummaries()) {
				ret.add(cur.getKey());
			}
		}catch(AmazonClientException e) {
			throw new ConnectorException("Failed to get objects list from amazon", e);
		}
		return ret;
	}

	/**
	 * Retrieves the data of the object with name <code>keyName</code> on the bucket
	 * 
	 * @param keyName key name of the target object
	 * @return InputStream contains the object data
	 */
	@Override
	public InputStream download(String keyName) {
		try {
			return _s3Client.getObject(_bucketName, keyName).getObjectContent();
		}catch(AmazonClientException e) {
			throw new ConnectorException("Failed to download object from amazon", e);
		}	
	}

	@Override
	public void upload(String path, String fileFormat, InputStream data, long dataLength) {
		ObjectMetadata metaData = new ObjectMetadata();
		metaData.setContentLength(dataLength);
		metaData.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		try {
			_s3Client.putObject(_bucketName, path + (_uploadCount++) + fileFormat, data, metaData);
		} catch(AmazonClientException e) {
			throw new ConnectorException("Failed to upload object to amazon", e);
		} finally {
			IOUtil.closeQuietly(data);
		}
	}

	/**
	 * Deletes an object with name <code>keyName</code> on the bucket
	 * 
	 * @param keyName key name of the target object
	 */
	@Override
	public void delete(String keyName)  {
		try {
			_s3Client.deleteObject(_bucketName, keyName);
		}catch(AmazonClientException e) {
			throw new ConnectorException("Failed to delete object from amazon", e);
		}	
	}

	/**
	 * Retrieves the url of the bucket with <code>prefixPath</code> appended
	 * 
	 * @param prefixPath the path to be appended to the bucket
	 * @return url of the AWS bucket
	 */
	@Override
	public String getStageUrl(String prefixPath) {
		return "'s3://" + _bucketName + "/" + prefixPath + "' ";
	}

	/**
	 * Retrieves the credentials to be used in Snowflake COPY INTO parameter
	 * 
	 * @return String contains the credentials parameter to a COPY INTO statement
	 */
	@Override
	public String getStageCredentials() {
		return SQL_COMMAND_CREDENTIALS + "=(" + SQL_COMMAND_AWS_ACCESS_KEY + "='" + getAccessKey()
				+ "' " + SQL_COMMAND_AWS_SECRET + "='" + getSecret() + "')" + SQL_COMMAND_ENCRYPTION;
	}

	/**
	 * Tests AWS bucket by making a listObjects request
	 * 
	 */
	@Override
	public void testConnection()  {
		try {
			_s3Client.listObjects(_bucketName, "Dummy");
		}catch(AmazonClientException e) {
			throw new ConnectorException("Failed to connect to AWS bucket", e);
		}
	}

	/**
	 * Tests AWS user credentials by making a listBuckets request using these credential and AWS
	 * DEFAULT_REGION
	 * 
	 * @param accessK AWS user Access Key
	 * @param secretK AWS user Secret Key
	 */
	public static void testConnectionCredentials(String accessK, String secretK){
		try {
			AWSCredentials credentials = new BasicAWSCredentials(accessK, secretK);
			AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION)
					.withCredentials(new AWSStaticCredentialsProvider(credentials)).build()
					.listBuckets();
		}catch(AmazonClientException e) {
			throw new ConnectorException("Failed to connect to AWS", e);
		}
	}

	/**
 * Uploads a file to a specified stage prefix, likely in an Amazon Web Services (AWS) environment.
 *
 * @param filePath The path of the file to be uploaded.
 * @param stagePrefix The prefix or location within the stage where the file should be uploaded.
 * @param dynamicPropertyMap A map containing dynamic properties that may be used during the upload process.
 * @return An empty string. This method is currently a placeholder and does not perform any actual upload.
 *
 * @implNote This method is currently a placeholder for future implementation in phase 2.
 *           It is intended to upload files from a user's device to AWS.
 *           The current implementation returns an empty string and does not perform any operations.
 */

	@Override
	public String upload(String filePath, String stagePrefix, DynamicPropertyMap dynamicPropertyMap) {
		// TODO in phase 2 upload files from user device to AWS
		return "";
	}

	private void initStreams(boolean compressionActivated) throws IOException {
		tempOutputStream = new TempOutputStream();
		meteredOutputStream = new MeteredOutputStream(tempOutputStream);
		if(compressionActivated) {
			gzipStream = new GZIPOutputStream(meteredOutputStream);	
		}
	}
	
	private void closeStreams(boolean compressionActivated) throws IOException {
		if(compressionActivated) {
			gzipStream.close();	
		}
		meteredOutputStream.close();
		tempOutputStream.close();
		gzipStream = null;
		meteredOutputStream = null;
		tempOutputStream = null;
	}
	
	private void writeToStream(boolean compressionActivated, byte curByte) throws IOException {
		if(compressionActivated) {
			gzipStream.write(curByte);
		}else {
			meteredOutputStream.write(curByte);
		}
	}
	
	@Override
	public void UploadHandler(String path, String fileFormat, InputStream data, long chunkSize, boolean compressionActivated, char recordDelimiter) {
		// input chunk size is in MB, so we multiply it by 2^20
		long chunkSizeInBytes = chunkSize << 20 ;
		try {
			initStreams(compressionActivated);
			int curByte;
			int openCurlyBrackets = 0;
			while ((curByte = data.read()) != -1) {
				if (curByte == '{') {
					openCurlyBrackets++;
				}
				handleChunkSize(path, fileFormat, compressionActivated, recordDelimiter, chunkSizeInBytes, curByte, openCurlyBrackets);
				if (curByte == '}') {
					openCurlyBrackets--;
				}
			}
			if (meteredOutputStream.getLength() != 0) {
				if (compressionActivated) {
					gzipStream.finish();
				}
				upload(path, fileFormat, tempOutputStream.toInputStream(), meteredOutputStream.getLength());
			}

		} catch (IOException e) {
			throw new ConnectorException("Compression Error", e);	
		} finally {
			IOUtil.closeQuietly(gzipStream);
			IOUtil.closeQuietly(meteredOutputStream);
			IOUtil.closeQuietly(tempOutputStream);
		}
	}

	private void handleChunkSize(String path, String fileFormat, boolean compressionActivated, char recordDelimiter,
			long chunkSizeInBytes, int curByte, int openCurlyBrackets) throws IOException {
		if ((meteredOutputStream.getLength() >= chunkSizeInBytes) && (fileFormat.contains("CSV") ? (curByte
				== recordDelimiter) : (openCurlyBrackets == 0))) {
			if (compressionActivated) {
				gzipStream.finish();
			}
			upload(path, fileFormat, tempOutputStream.toInputStream(), meteredOutputStream.getLength());
			closeStreams(compressionActivated);
			initStreams(compressionActivated);
		} else {
			writeToStream(compressionActivated, (byte) curByte);
		}
	}
}
