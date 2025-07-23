/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.text.StringEscapeUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.awsmodel.AWSPutEventRequest;
import com.boomi.connector.model.PutEventRequest;
import com.boomi.connector.model.PutEventRequestType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The AWSEventBridgeUtil class is an utility class.
 * 
 * @author a.kumar.samantaray
 * 
 */
public class AWSEventBridgeUtil {

	/** The Constant ZERO. */
	private static final String ZERO = "0";

	/** The Constant KEYPATHREP. */
	private static final String KEYPATHREP = "%2F";

	/** The Constant mapper. */
	private static final ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

	/**
	 * Instantiates a new AWS event bridge util.
	 */
	private AWSEventBridgeUtil() {

	}

	/**
	 * This method returns the Mapped collection of standard header for AWS HTTP
	 * Request.
	 * 
	 * @return Response as Map<String, String>
	 */
	public static Map<String, String> createHeader() {
		SecureRandom random = new SecureRandom();
		Map<String, String> map = new HashMap<>();
		map.put(AWSEventBridgeConstant.HOST, AWSEventBridgeConstant.HOSTVALUE);
		map.put(AWSEventBridgeConstant.CONTENTTYPE, AWSEventBridgeConstant.CONTENTTYPEVALUE);
		map.put(AWSEventBridgeConstant.AWSREQID, new UUID(random.nextLong(), random.nextLong()).toString());
		map.put(AWSEventBridgeConstant.USERAGENT, AWSEventBridgeConstant.USERAGENTVALUE);
		return map;
	}

	/**
	 * This method takes the byte array and return the Hexadecimal value.
	 *
	 * @param data the data
	 * @return Response as String
	 */
	public static String toHex(byte[] data) {
		StringBuilder sb = new StringBuilder(data.length * 2);
		for (int i = 0; i < data.length; i++) {
			String hex = Integer.toHexString(data[i]);
			if (hex.length() == 1) {
				sb.append(ZERO);
			} else if (hex.length() == 8) {
				hex = hex.substring(6);
			}
			sb.append(hex);
		}
		return sb.toString().toLowerCase(Locale.getDefault());
	}

	/**
	 * It takes a String object, convert the String as per the Put event required
	 * format.
	 *
	 * @param s the s
	 * @return {@link String}
	 */
	public static String convertString(String s) {
		String[] r = s.split(AWSEventBridgeConstant.UNDERSCORE);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < r.length; i++) {
			sb.append(formatString(r[i]));
		}
		return sb.toString();
	}

	/**
	 * It takes a String object, convert the String as per the Put event required
	 * format.
	 *
	 * @param str the str
	 * @return {@link String}
	 */
	private static String formatString(String str) {
		return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
	}

	/**
	 * Convert the format of the JSON which is received from the connector to AWS
	 * required format type List object.
	 *
	 * @param putEvents the put events
	 * @param region    the region
	 * @param accountId the account id
	 * @return List<AWSPutEventRequest>
	 * @throws JsonProcessingException the json processing exception
	 */
	public static List<AWSPutEventRequest> convertRequestMessageforPutEvent(PutEventRequestType putEvents,
			String region, String accountId) throws JsonProcessingException {
		List<AWSPutEventRequest> listAwsRequest = new ArrayList<>();
		for (PutEventRequest inpReq : putEvents.getRequest()) {
			AWSPutEventRequest awsReq = new AWSPutEventRequest();
			awsReq.setEventbusName(inpReq.getEventbusName());
			if (inpReq.getDetail() instanceof String) {
				awsReq.setDetail(StringEscapeUtils.escapeJava(escapeCharacters(inpReq.getDetail().toString(),
						Collections.unmodifiableList(Arrays.asList("\n", "\r", "\t")))));
			} else {
				awsReq.setDetail(
						StringEscapeUtils.escapeJava(escapeCharacters(mapper.writeValueAsString(inpReq.getDetail()),
								Collections.unmodifiableList(Arrays.asList("\n", "\r", "\t")))));
			}
			awsReq.setDetailType(inpReq.getDetailType());
			awsReq.setResources(buildResources(inpReq.getEventbusName(), region, accountId));
			awsReq.setSource(inpReq.getSource());
			listAwsRequest.add(awsReq);
		}
		return listAwsRequest;
	}

	/**
	 * This method will escape the Characters inorder to meet the Post Construct
	 * Policy of the Amazon
	 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html}
	 *
	 * @param detail      the detail
	 * @param escapeChars the escape chars
	 * @return the string
	 */
	private static String escapeCharacters(String detail, List<String> escapeChars) {
		for (String escape : escapeChars) {
			if (detail.contains(escape)) {
				detail = detail.replace(escape, "");
			}
		}
		return detail;
	}

	/**
	 * creates a resource string for AWS PUT Event request body using the bus,
	 * region and account id.
	 *
	 * @param bus       the bus
	 * @param region    the region
	 * @param accountId the account id
	 * @return List<String>
	 */
	private static List<String> buildResources(String bus, String region, String accountId) {
		List<String> list = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		sb.append("arn:aws:events:");
		sb.append(region);
		sb.append(AWSEventBridgeConstant.COLON);
		sb.append(accountId);
		sb.append(":event-bus/");
		sb.append(bus);
		list.add(sb.toString());
		return list;
	}

	/**
	 * This method encodes the URL.
	 *
	 * @param url           the url
	 * @param keepPathSlash the keep path slash
	 * @return String encoding value of URL
	 */
	public static String urlEncode(String url, boolean keepPathSlash) {
		String encoded;
		try {
			encoded = URLEncoder.encode(url, AWSEventBridgeConstant.UTF8);
		} catch (UnsupportedEncodingException e) {
			throw new ConnectorException(AWSEventBridgeConstant.UTFENCODINGERROR, e);
		}
		if (keepPathSlash) {
			encoded = encoded.replace(KEYPATHREP, AWSEventBridgeConstant.FWDSLASH);
		}
		return encoded;
	}
}
