//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static com.boomi.connector.liveoptics.utils.LiveOpticsConstants.*;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;
import org.json.JSONObject;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.liveoptics.utils.JsonSchemaBuilder;
import com.boomi.connector.liveoptics.utils.LiveOpticsResponse;
import com.boomi.connector.liveoptics.utils.Rfc2898DeriveBytes;
import com.boomi.connector.util.BaseConnection;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * @author Naveen Ganachari
 *
 *         ${tags}
 */
public class LiveOpticsConnectorConnection extends BaseConnection {

	private final String _baseUrl;
	private final String mLoginSecret;
	private final String mSharedSecret;
	private final String mLoginId;
	private final Boolean mIncludeEntities;
	private static int DATE_START_IDX = 0; // initialize as class variables
	private static int DATE_LENGTH = 14;
	private static int EXPECTED_LENGTH = 345;
	private static int EXPECTED_HMAC_LENGTH = 88;
	private static int IV_START_IDX = 14;
	private static int IV_LENGTH = 38;
	private static int AES_IV_LENGTH = 16;
	private static int CIPHERTEXT_START_IDX = 38;
	private static int CIPHERTEXT_LENGTH = 254;
	private static int EXPECTED_VERSION_LENGTH = 1;
	private static int EXPECTED_BLOB_LENGTH = 254;
	private static String EXPECTED_VERSION = "1";
	private static int HMAC_INPUT_START_IDX = 0;
	private static int HMAC_SEPARATOR_LENGTH = 1;
	private static int HMAC_INPUT_LENGTH = EXPECTED_LENGTH - EXPECTED_HMAC_LENGTH - HMAC_SEPARATOR_LENGTH;

	public LiveOpticsConnectorConnection(BrowseContext context) {
		super(context);

		PropertyMap pm = context.getConnectionProperties();

		this._baseUrl = getBaseUrl(pm);
		this.mLoginSecret = pm.getProperty(LOGIN_SECRET);
		this.mSharedSecret = pm.getProperty(SHARED_SECRET);
		this.mLoginId = pm.getProperty(LOGIN_ID_1);
		this.mIncludeEntities = context.getOperationProperties().getBooleanProperty(INCLUDE_ENTITIES);
	}

	
	/**
	 * This method generates schema on the basis of project ID passed
	 * 
	 * @return String The metadata
	 * @throws IOException
	 */
	public String getMetadata() throws IOException {
		return JsonSchemaBuilder.buildJsonSchema(_baseUrl + METADATA_RESOURCE);
	}

	
	/**
	 * This method hits the login API URL and accepts login ID and login token as
	 * inputs and generates a response
	 * 
	 * @return LiveOpticsResponse
	 * @throws IOException
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws JSONException
	 */
	public LiveOpticsResponse doLogin()
			throws IOException, InvalidKeyException, NoSuchAlgorithmException, JSONException {

		HttpURLConnection conn = null;
		LiveOpticsResponse loginResponse = null;
		try {
			URL url = new URL(_baseUrl + LOGIN_API);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(POST_METHOD);
			conn.setRequestProperty(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE);
			conn.setRequestProperty("Accept", JSON_CONTENT_TYPE);

			JSONObject json = new JSONObject();
			json.put(LOGIN_ID, mLoginId);
			json.put(LOGIN_TOKEN, generateLoginToken(mLoginId, Base64.decodeBase64(mLoginSecret.getBytes())));
			String jsonInputString = json.toString();
			conn.setDoOutput(true);
			conn.setDoInput(true);

			try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream());) {
				wr.write(jsonInputString.getBytes());
				wr.flush();
			}
			loginResponse = new LiveOpticsResponse();
			getLiveOpticsResponse(conn, loginResponse);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return loginResponse;
	}

	
	/**
	 * This method populate the LiveOptics response object from the response we are
	 * getting from Live Optics
	 * 
	 * @param conn The HTTP URL Connection
	 * @param response The LiveOpticsResponse
	 * @throws IOException
	 */
	public void getLiveOpticsResponse(HttpURLConnection conn, LiveOpticsResponse response) throws IOException {
		if (conn != null) {
			try (InputStream responseStream = conn.getInputStream();) {
				response.setResponseCode(conn.getResponseCode());
				response.setResponseMessage(conn.getResponseMessage());
				if (response.getStatus() == OperationStatus.SUCCESS) {
					response.setSessionToken(getToken(responseStream));
				}
			}
		}
	}
	
	/*Parsing the Token value and returns it.*/
	private String getToken(InputStream responseStream) throws JsonParseException, IOException {
		try (JsonParser jp = new JsonFactory().createParser(responseStream);) {
			while (jp.nextToken() != null) {
				String fieldName = jp.getCurrentName();
				if (fieldName != null && fieldName.equals("Token")) {
					JsonToken nextToken = jp.nextToken();
					if (!nextToken.equals(JsonToken.VALUE_NULL)) {
						return jp.getText();
					}
				}
			}
		}
		return null;
	}

	
	
	/**
	 * This method hits the close API URL, accepts session ID as the input and
	 * generates a success or failure response.
	 * 
	 * @param mSessionToken The session token of type String
	 * @return LiveOpticsResponse
	 * @throws IOException
	 * @throws JSONException
	 */
	public LiveOpticsResponse doClose(String mSessionToken) throws IOException, JSONException {
		HttpURLConnection conn = null;
		LiveOpticsResponse closeResponse = null;
		try {
			URL url = new URL(_baseUrl + CLOSE_API);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(POST_METHOD);
			conn.setRequestProperty(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE);
			conn.setRequestProperty("Accept", JSON_CONTENT_TYPE);

			JSONObject json = new JSONObject();
			json.put("Session", mSessionToken);
			String jsonInputString = json.toString();
			conn.setDoOutput(true);
			conn.setDoInput(true);

			try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream());) {
				wr.write(jsonInputString.getBytes());
				wr.flush();
			}
			closeResponse = new LiveOpticsResponse();
			getLiveOpticsResponse(conn, closeResponse);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return closeResponse;
	}

	private String getBaseUrl(PropertyMap props) {
		return props.getProperty(URL_PROPERTY, DEFAULT_URL);
	}

		
	/**
	 * 
	 * This method generates a base 64 encoded login HMAC token based on login ID
	 * and login secret key
	 * 
	 * @param loginId The login id
	 * @param loginSecret The login secret
	 * @return String The generated token
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws UnsupportedEncodingException
	 */
	public String generateLoginToken(String loginId, byte[] loginSecret)
			throws NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException {

		SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		String now = df.format(new Date());

		StringBuilder buff = new StringBuilder();
		buff.append(loginId);
		buff.append(now);
		byte[] hmacInputBytes = buff.toString().getBytes(StandardCharsets.US_ASCII);
		Mac sha512_HMAC = Mac.getInstance(HMAC_SHA512);
		SecretKeySpec keySpec = new SecretKeySpec(loginSecret, HMAC_SHA512);
		sha512_HMAC.init(keySpec);
		sha512_HMAC.update(hmacInputBytes);
		String loginHmac = Base64.encodeBase64String(sha512_HMAC.doFinal());

		StringBuilder output = new StringBuilder(now);
		output.append(loginHmac);
		return output.toString();
	}

	/**
	 * This method decrypts the shared secret key based on various validations
	 * 
	 * @param maybeSession The maybeSession of type String
	 * @param sharedKey The shared key of type String
	 * @param loginSecret The login Secret
	 * @return String Decrypted String
	 * @throws ArgumentException
	 * @throws ArgumentNullException
	 */
	public String parse(String maybeSession, String sharedKey, byte[] loginSecret)
			throws ArgumentException, ArgumentNullException {
		if (maybeSession == null)
			throw new ArgumentNullException("maybeSession");

		if (maybeSession.length() != EXPECTED_LENGTH)
			throw new ArgumentException("maybeSession must be " + EXPECTED_LENGTH + " characters");

		String[] lParts = maybeSession.split("\\|");
		if (lParts.length != 3) {
			throw new ArgumentException("maybeSession is not a valid session string");
		}
		String lVersion = lParts[0];
		String lSessionBlob = lParts[1];
		String lHmac = lParts[2];

		if (lVersion.length() != EXPECTED_VERSION_LENGTH && !lVersion.equalsIgnoreCase(EXPECTED_VERSION)) {
			throw new ArgumentException("maybeSession is an unrecognized version");
		}

		if (lSessionBlob.length() != EXPECTED_BLOB_LENGTH) {
			throw new ArgumentException("maybeSession blob length is incorrect");
		}

		if (lHmac.length() != EXPECTED_HMAC_LENGTH) {
			throw new ArgumentException("maybeSession HMAC length is incorrect");
		}

		String hmacInput = maybeSession.substring(HMAC_INPUT_START_IDX, HMAC_INPUT_LENGTH);

		try {
			if (!validateHmac(hmacInput, lHmac, loginSecret)) {
				throw new ArgumentException("maybeSession HMAC does not match");
			}
		} catch (Exception e) {
			throw new ArgumentException("Exception while validating the HMAC instance against the maybeSession HMAC");
		}

		String datestamp = lSessionBlob.substring(DATE_START_IDX, DATE_LENGTH);

		byte[] iv = null;
		try {
			String b64iv = lSessionBlob.substring(IV_START_IDX, IV_LENGTH);
			iv = Base64.decodeBase64(b64iv.getBytes());
		} catch (Exception e) {
			throw new ArgumentException("IV is not a valid base-64 encoded string", e);
		}

		if (iv.length != AES_IV_LENGTH) {
			throw new ArgumentException("Decoded IV is not the expected length");
		}
		byte[] cipherText = null;
		try {
			String b64Cipher = lSessionBlob.substring(CIPHERTEXT_START_IDX, CIPHERTEXT_LENGTH);
			cipherText = Base64.decodeBase64(b64Cipher.getBytes());
		} catch (Exception e) {
			throw new ArgumentException("Cipher text is not a valid base-64 encoded string", e);
		}

		return decrypt(datestamp, iv, cipherText, sharedKey);

	}

	/*
	 * This method is used to generate a decrypted shared secret key
	 */
	private String decrypt(String issuedAt, byte[] iv, byte[] cipherText, String sharedSecret)
			throws ArgumentException {
		try {
			byte[] salt = issuedAt.getBytes(StandardCharsets.US_ASCII);
			return decodeAndDecrypt(cipherText, iv, salt, sharedSecret);
		} catch (Exception ex) {
			throw new ArgumentException("Exception while decrypting shared secret key", ex);
		}

	}

	
	/**
	 * This method decodes and decrypts the encrypted secret
	 * 
	 * @param encrypted The encrypted value as byte array
	 * @param IV The IV value of type byte array
	 * @param salt The salt value of type byte array
	 * @param sharedSecret The shared secret of type String
	 * @return String The decrypted String
	 * @throws ArgumentException
	 */
	public String decodeAndDecrypt(byte[] encrypted, byte[] IV, byte[] salt, String sharedSecret)
			throws ArgumentException {
		Cipher c = getCipher(Cipher.DECRYPT_MODE, sharedSecret, salt, IV);
		try {
			byte[] decValue;
			decValue = c.doFinal(encrypted);
			return new String(decValue, StandardCharsets.US_ASCII);
		} catch (Exception ex) {
			throw new ArgumentException("Exception while decrypting the Session Id", ex);
		}
	}

	private Cipher getCipher(int mode, String PASSWORD, byte[] SALT, byte[] IV) throws ArgumentException {
		Cipher c;
		try {
			c = Cipher.getInstance("AES/CBC/PKCS5Padding");
			c.init(mode, generateKey(PASSWORD, SALT), new IvParameterSpec(IV));
			return c;
		} catch (Exception e) {
			throw new ArgumentException("Exception while getting Cipher", e);
		}
	}

	/*
	 * This method generates the AES key based on the password we pass as shared
	 * secret key and salt which is a timestamp.
	 */
	private Key generateKey(String PASSWORD, byte[] SALT) throws ArgumentException {
		Rfc2898DeriveBytes deriveBytes;
		try {
			deriveBytes = new Rfc2898DeriveBytes(Base64.decodeBase64(PASSWORD), SALT, 1000);
			byte[] derivedBytes = deriveBytes.getBytes(32);
			return new SecretKeySpec(derivedBytes, "AES");
		} catch (Exception e) {
			throw new ArgumentException("Key generation failed.");
		}
	}

	/*
	 * This method will validate the HMAC instance against the maybeSession HMAC
	 */
	private static Boolean validateHmac(String hmacInput, String b64MaybeHmac, byte[] loginSecret)
			throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
		byte[] hmacInputBytes = hmacInput.getBytes(StandardCharsets.US_ASCII);
		Mac sha512_HMAC = Mac.getInstance(HMAC_SHA512);
		SecretKeySpec keySpec = new SecretKeySpec(loginSecret, HMAC_SHA512);
		sha512_HMAC.init(keySpec);
		sha512_HMAC.update(hmacInputBytes);
		String loginHmac = Base64.encodeBase64String(sha512_HMAC.doFinal());
		return loginHmac.equals(b64MaybeHmac);
	}

	/**
	 * Get the Base URL
	 * 
	 * @return String The base URL
	 */
	public String getBaseUrl() {
		return this._baseUrl;
	}

	/**
	 * Get the Shared Secret
	 * 
	 * @return String The shared secret
	 */
	public String getSharedSecret() {
		return this.mSharedSecret;
	}

	
	/**
	 * Get the Login Secret
	 * 
	 * @return String The login secret
	 */
	public String getLoginSecret() {
		return this.mLoginSecret;
	}

	/**
	 * Include Entities
	 * 
	 * @return Object The entities
	 */
	public Object getIncludeEntities() {
		return this.mIncludeEntities;
	}
}
