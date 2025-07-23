// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome.util;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class DellOMEIPSSLContext {

	public static TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
		public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			return null;
		}

		public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {

		}

		public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {

		}
	} };

	// Use allHostValid to bypass SSL handshake if no valid certificates
	public static HostnameVerifier allHostsValid = new HostnameVerifier() {
		public boolean verify(String hostname, SSLSession session) {
			return true;
		}
	};

	public static SSLContext getInstance() throws KeyManagementException, NoSuchAlgorithmException {
		return getInstance("SSL");
	}

	public static SSLContext getInstance(String protocol) throws KeyManagementException, NoSuchAlgorithmException {
		SSLContext sc = SSLContext.getInstance(protocol);
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		return sc;
	}

}
