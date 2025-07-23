// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.util;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.util.StringUtil;

/**
 * Implements logic to generate fetch the certificate.
 * 
 */
public class CertificateUtils {

	/**
	 * Instantiates a new certificate utils.
	 */
	private CertificateUtils() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Prints the aliases.
	 *
	 * @param keyStore the key store
	 * @return the string buffer
	 */
	public static StringBuilder printAliases(KeyStore keyStore) {
		StringBuilder aliasesDet = new StringBuilder("Aliases in keystore");
		Enumeration<String> aliases = null;
		int count = 0;
		try {
			aliases = keyStore.aliases();
		} catch (KeyStoreException e) {
			aliasesDet.append(" - ").append(e.getMessage());
		}
		while (null != aliases && aliases.hasMoreElements()) {
			aliasesDet.append(" - ").append(aliases.nextElement());
			count++;
		}
		aliasesDet.append(" - ").append(count);
		return aliasesDet;
	}

	/**
	 * Fetch the subject of the certficate.
	 *
	 * @param keyStore the key store
	 * @param alias    the alias
	 * @param logger   the logger
	 * @return the string
	 */
	public static String fetchCertSubject(KeyStore keyStore, String alias, Logger logger) {
		String subjectName = null;
		Principal subject = null;
		Certificate certificate = null;
		try {
			certificate = keyStore.getCertificate(alias);
		} catch (KeyStoreException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
		if (null != certificate) {
			if (certificate instanceof X509Certificate) {
				subject = ((X509Certificate) certificate).getSubjectDN();
				if (null != subject) {
					subjectName = subject.toString().replaceAll("\\s+", StringUtil.EMPTY_STRING);
				} else {
					logger.log(Level.SEVERE,"Error while fetching subjectName from certificate of alias- {0} | - X509Certificate.getsubjectDN returned NULL ", new Object[] {alias});
				}
			} else {
				logger.log(Level.SEVERE, "Error while fetching subjectName from certificate of alias: {0} | - certificate of type- {1} | - {2}", new Object[] {alias,certificate.getType(),certificate.getClass().getName()});
			}
		}
		return subjectName;
	}

}
