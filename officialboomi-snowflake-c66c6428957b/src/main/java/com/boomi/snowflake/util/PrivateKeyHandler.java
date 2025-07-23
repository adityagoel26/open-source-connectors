// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import com.boomi.util.StringUtil;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.IOException;
import java.io.StringReader;
import java.security.InvalidKeyException;
import java.security.PrivateKey;
import java.security.Security;

public class PrivateKeyHandler {

    private static final String PROP_BEGIN_ENCRYPTED =
            "-----BEGIN ENCRYPTED PRIVATE KEY-----" + StringUtil.LINE_SEPARATOR;
    private static final String PROP_END_ENCRYPTED = StringUtil.LINE_SEPARATOR + "-----END ENCRYPTED PRIVATE KEY-----";
    private static final String PROP_BEGIN_ENCRYPTED_LINE_SEPARATOR = StringUtil.normalizeWhitespace(
            PROP_BEGIN_ENCRYPTED, StringUtil.LINE_SEPARATOR);
    private static final String PROP_END_ENCRYPTED_LINE_SEPARATOR = StringUtil.normalizeWhitespace(PROP_END_ENCRYPTED,
            StringUtil.LINE_SEPARATOR);
    private static final String PROP_BEGIN = "BEGIN";
    private static BouncyCastleProvider BOUNCY_CASTLE_PROVIDER;

    private PrivateKeyHandler() {
        // Prevent initialization
    }

    /**
     * This method is used to handle the case where the private key is encrypted or Handle the case where the private
     * key is unencrypted.
     *
     * @param privateKeyString The PEM-encoded private key string.
     * @param passphrase       The passphrase used to decrypt the private key, if encrypted.
     * @return The PrivateKey object extracted from the PEM-encoded string.
     * @throws InvalidKeyException       If the private key is invalid or cannot be processed.
     * @throws IOException               If an I/O error occurs while reading the private key.
     * @throws OperatorCreationException If an error occurs during the creation of cryptographic operators.
     * @throws PKCSException             If an error occurs while handling PKCS#8 private key information.
     */
    public static PrivateKey getPrivateObject(String privateKeyString, char[] passphrase)
            throws InvalidKeyException, IOException, OperatorCreationException, PKCSException {

        try (StringReader reader = new StringReader(getPrivateKey(privateKeyString));
                PEMParser pemParser = new PEMParser(reader)) {
            if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
                Security.addProvider(getProviderInstance());
            }
            Object pemObject = pemParser.readObject();
            PrivateKeyInfo privateKeyInfo = getPrivateKeyInfo(passphrase, pemObject);
            // Convert PrivateKeyInfo to PrivateKey
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(getProviderInstance());
            return converter.getPrivateKey(privateKeyInfo);
        }
    }

    private static BouncyCastleProvider getProviderInstance() {
        BouncyCastleProvider bouncyCastleProviderRef = BOUNCY_CASTLE_PROVIDER;
        if (bouncyCastleProviderRef == null) {
            synchronized (PrivateKeyHandler.class) {
                bouncyCastleProviderRef = BOUNCY_CASTLE_PROVIDER;
                if (bouncyCastleProviderRef == null) {
                    BOUNCY_CASTLE_PROVIDER = bouncyCastleProviderRef = new BouncyCastleProvider();
                }
            }
        }
        return bouncyCastleProviderRef;
    }

    private static String getPrivateKey(String privateKeyString) {
        String normalizedPrivateKeyString = privateKeyString;
        if (!privateKeyString.contains(PROP_BEGIN)) {
            normalizedPrivateKeyString = PROP_BEGIN_ENCRYPTED + privateKeyString + PROP_END_ENCRYPTED;
        }
        // Normalize the private key string by replacing multiple whitespaces with a single line separator
        normalizedPrivateKeyString = StringUtil.normalizeWhitespace(normalizedPrivateKeyString,
                StringUtil.LINE_SEPARATOR);
        if (normalizedPrivateKeyString.contains(PROP_BEGIN_ENCRYPTED_LINE_SEPARATOR)) {
            // Replace encrypted line separators with standard markers
            normalizedPrivateKeyString = normalizedPrivateKeyString.replace(PROP_BEGIN_ENCRYPTED_LINE_SEPARATOR,
                    PROP_BEGIN_ENCRYPTED).replace(PROP_END_ENCRYPTED_LINE_SEPARATOR, PROP_END_ENCRYPTED);
        }
        // Return the processed and normalized private key string
        return normalizedPrivateKeyString;
    }

    private static PrivateKeyInfo getPrivateKeyInfo(char[] passphrase, Object pemObject)
            throws OperatorCreationException, PKCSException, InvalidKeyException {
        if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
            // Handle the case where the private key is encrypted.
            PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
            InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().setProvider(
                    getProviderInstance()).build(passphrase);
            return encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
        }
        throw new InvalidKeyException("Incorrect private key string or passphrase");
    }
}
