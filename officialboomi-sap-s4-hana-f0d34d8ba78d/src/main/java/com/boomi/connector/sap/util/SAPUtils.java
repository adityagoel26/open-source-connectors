// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.sap.SAPConnectorConnection;

/**
 * @author kishore.pulluru
 *
 */
public class SAPUtils {

	private static final Logger logger = Logger.getLogger(SAPUtils.class.getName());

	private static final String PLACEHOLDER = "ApiNamePlaceHolder";

	private static final String SAP_API_URL = "https://api.sap.com/odata/1.0/catalog.svc/APIContent.APIs('ApiNamePlaceHolder')/$value?type=JSON&attachment=true";

	private SAPUtils() {

	}
	
	public static String encodeUrl(String surl) {
		 try {
			surl=URLEncoder.encode( surl, "UTF-8" );
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.SEVERE, "Unable to encode the URL : {0} ",e.getMessage());
		}
		 return surl;
	}

	/**
	 * This method will get the swagger document name from the given URL.
	 * 
	 * @param sUrl
	 * @return Swagger Document Name
	 */
	public static String getSwaggerDocumentName(String sUrl) {
		sUrl = sUrl.substring(sUrl.lastIndexOf('/') + 1, sUrl.length());
		if (getApiNamesForSwagger().containsKey(sUrl)) {
			sUrl = getApiNamesForSwagger().get(sUrl);
		}
		return sUrl;
	}

	/**
	 * This method will return names of swagger documents if it is not matching with
	 * existing name.
	 * 
	 * @return Swagger Document Names Map
	 */
	private static Map<String, String> getApiNamesForSwagger() {
		final Map<String, String> map = new HashMap<>();
		map.put("API_INBOUND_DELIVERY_SRV;v=0002", "API_INBOUND_DELIVERY_SRV_0002");
		map.put("SC_PROJ_ENGMT_CREATE_UPD_SRV", "_CPD_SC_PROJ_ENGMT_CREATE_UPD_SRV");
		map.put("A_SUPPLIEROPLSCORESAV_CDS", "API_SUPPLIEROPLSCORESAV_CDS");
		map.put("A_TRSYPOSTGJRNLENTRITM_CDS", "API_TRSYPOSTGJRNLENTRITM_SRV");
		map.put("SC_EXTERNAL_SERVICES_SRV", "_CPD_SC_EXTERNAL_SERVICES_SRV");
		map.put("API_CUSTOMER_RETURNS_DELIVERY_SRV;v=0002", "API_CUSTOMER_RETURNS_DELIVERY_SRV");
		map.put("API_CHANGEMASTER;v=0002", "API_CHANGEMASTER_0002");
		map.put("API_BILL_OF_MATERIAL_SRV;v=0002", "API_BILL_OF_MATERIAL_SRV_0002");
		map.put("BC_EXT_APPJOB_MANAGEMENT;v=0002", "BC_EXT_APPJOB_MANAGEMENT");
		map.put("API_OUTBOUND_DELIVERY_SRV;v=0002", "API_OUTBOUND_DELIVERY_SRV_0002");
		map.put("API_KANBAN_CONTROL_CYCLE_SRV;v=0002", "API_KANBAN_CONTROL_CYCLE_SRV_0002");
		map.put("A_TRSYPOSFLOW_CDS", "API_TRSYPOSFLOW_SRV");
		return map;
	}

	/**
	 * This method will get the swagger document by making the HTTP call to the
	 * server.
	 * 
	 * @param apiName
	 * @param userName
	 * @param password
	 * @return Swagger Document
	 */
	public static String getSwaggerDocument(String apiName, String userName, String password) {

		URL url = null;
		InputStream httpResponse = null;
		String swaggerDocument = null;

		if (isValidInput()) {
			HttpURLConnection urlConnection = null;
			String userpass = userName + ":" + password;
			String authorization = new String(Base64.getEncoder().encode(userpass.getBytes()));
			authorization = "Basic " + authorization;
			try {
				String surl = SAP_API_URL.replace(PLACEHOLDER, apiName);
				url = new URL(surl);
				urlConnection = (HttpURLConnection) url.openConnection();
				urlConnection.setRequestMethod(Action.GET);
				urlConnection.setRequestProperty("Content-Type", "application/json");
				urlConnection.setRequestProperty("Accept", "application/json");
				urlConnection.setRequestProperty("Authorization", authorization);

				httpResponse = urlConnection.getInputStream();
				swaggerDocument = convertIStoString(httpResponse);
			} catch (Exception e) {
				throw new ConnectorException("Exception while getting the Swagger Document." + e.getMessage());
			} finally {
				try {
					if (null != httpResponse) {
						httpResponse.close();
					}
					if (null != urlConnection) {
						urlConnection.disconnect();
					}
				} catch (IOException e) {
					logger.warning(SAPConstants.CONNECTION_LEAK_MSG);
				}
			}
		} else {
			throw new ConnectorException("Please Provide Valid Credentials.");
		}

		return swaggerDocument;
	}

	/**
	 * This method will check weather given parameters are valid or not i.e (not
	 * null, not blank)
	 * 
	 * @param params
	 * @return true if the input is valid
	 */
	public static boolean isValidInput(String... params) {
		for (String param : params) {
			if (StringUtils.isBlank(param)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * This method will convert ObjectData to Byte Array.
	 * 
	 * @param objdata
	 * @return byte[]
	 */
	public static byte[] convertToByteArray(ObjectData objdata) {

		byte[] data = null;
		try (ByteArrayOutputStream buffer = new ByteArrayOutputStream(); InputStream is = objdata.getData();) {

			int nRead;
			data = new byte[is.available()];

			while ((nRead = is.read(data, 0, data.length)) != -1) {
				buffer.write(data, 0, nRead);
			}
			data = buffer.toByteArray();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Exception while converting ObjectData to Byte Array");
		}

		return data;
	}

	/**
	 * this method will convert inputstream to string.
	 * 
	 * @param inputStream
	 * @return converted string
	 * @throws IOException
	 */
	public static String convertIStoString(InputStream inputStream) throws IOException {

		StringBuilder stringBuilder = new StringBuilder();
		String line = null;

		Charset charset = StandardCharsets.UTF_8;

		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, charset))) {
			while ((line = bufferedReader.readLine()) != null) {
				stringBuilder.append(line);
			}
		}
		return stringBuilder.toString();
	}

	public static List<InputStream> getChunkedInputStreams(InputStream is, int maxChunkSize) throws IOException {
		List<InputStream> inputStreamList = new ArrayList<>();
		if (is.available() <= maxChunkSize) {
			inputStreamList.add(is);
		} else {
			int from = 0;
			int to = maxChunkSize;
			byte[] barray = IOUtils.toByteArray(is);

			while (true) {
				byte[] chunk = Arrays.copyOfRange(barray, from, to);
				InputStream temp = new ByteArrayInputStream(chunk);
				inputStreamList.add(temp);
				from = to + 1;
				to = Math.min(from + maxChunkSize, barray.length);

				if (from == barray.length + 1) {
					break;
				}
			}
		}

		return inputStreamList;
	}
	
	public static String getBasicAuthToken(SAPConnectorConnection con) {
		logger.log(Level.INFO,"SAP UserId : {0}",con.getSapUserId());
		String userpass = con.getSapUserId() + ":" + con.getSapPassword();
		String authorization = new String(Base64.getEncoder().encode(userpass.getBytes()));
		return "Basic " + authorization;
	}

}
