//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.requests;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.IOUtil;
import com.boomi.util.security.SecurityUtil;

import org.apache.http.conn.ssl.SSLContexts;

import javax.net.ssl.SSLContext;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;


/**
 * @author saurav.b.sengupta
 *
 */
public class RequestContextHelper {

    private static final String DIGI_CERT_GLOBAL_ROOT_G2_CER = "DigiCertGlobalRootG2.cer";
    private static final String DIGI_CERT_GLOBAL_ROOT_G2_CER_test = "src/test/resources/"+"DigiCertGlobalRootG2.cer";
    private static final String ALIAS = "digicert";
    private static final String X_509 = "X.509";

    public static void setSSLContextForTest() {
       
			//Requester.setSSLContext(readSSLContext());
	
    }

    private static SSLContext readSSLContext() {
        InputStream is = null;
        try {
        	is=new FileInputStream(DIGI_CERT_GLOBAL_ROOT_G2_CER_test);
            KeyStore trustStore = SecurityUtil.loadCaCerts();
            trustStore.setCertificateEntry(ALIAS, CertificateFactory.getInstance(X_509).generateCertificate(is));
            SSLContext builtSSLContext = SSLContexts.custom().loadTrustMaterial(trustStore).build();
            return builtSSLContext;
        }
        catch (Exception e) {
            throw new ConnectorException(e);
        }
        finally {
           IOUtil.closeQuietly(is);
        }
    }

}
