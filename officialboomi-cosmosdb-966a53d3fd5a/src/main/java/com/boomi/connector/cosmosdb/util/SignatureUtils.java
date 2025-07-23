//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.util;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.SimpleTimeZone;

import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class SignatureUtils {

	private SignatureUtils() {
	}

	/**
	 * This method is used for creating Request Headers needed to connect to Cosmos
	 * DB API
	 * 
	 * @param uri
	 * @param masterKey
	 * @param httpMethod
	 * @return Map<String, String>
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws UnsupportedEncodingException
	 */
	public static Map<String, String> generateHashSignature(String uri, String masterKey, String httpMethod)
			throws NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException {

		HashMap<String, String> headerParameters = new HashMap<>();
		String authorization = null;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(X_MS_DATE_FORMAT, Locale.US);
		simpleDateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));
		String currentDate = simpleDateFormat.format(new Date());
		headerParameters.put(RFC_TIME, currentDate);
		URI parsedUri = URI.create(uri);
		String strippedurl = parsedUri.getPath();
		String[] strippedparts = strippedurl.split("/");
		int truestrippedcount = (strippedparts.length - 1);
		String resourceId = "";
		String resourceType = "";
		if (truestrippedcount % 2 != 0) {
			resourceType = strippedparts[truestrippedcount];
			if (truestrippedcount > 1) {
				int lastPart = strippedurl.lastIndexOf('/');
				resourceId = strippedurl.substring(1, lastPart);
			}
		} else {
			resourceType = strippedparts[truestrippedcount - 1];
			strippedurl = strippedurl.substring(1);
			resourceId = strippedurl;
		}
		String payload = httpMethod.toLowerCase() + LINE_BREAK + resourceType.toLowerCase()
				+ LINE_BREAK + resourceId + LINE_BREAK + currentDate.toLowerCase()
				+ LINE_BREAK + "" + LINE_BREAK;
		Mac mac;
		mac = Mac.getInstance(SIGNATURE_ALGORITHM);
		SecretKeySpec secretKey = new SecretKeySpec(Base64.decodeBase64(masterKey),
				SIGNATURE_ALGORITHM);
		mac.init(secretKey);
		String signature = Base64.encodeBase64String(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
		authorization = URLEncoder.encode(SIGNATURE_PARAMS + signature, UTF);
		headerParameters.put(AUTH_TOKEN, authorization);
		return headerParameters;
	}

}
