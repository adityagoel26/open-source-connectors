// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.StringUtil;
import com.sap.conn.jco.util.Codecs;

/**
 * 
 * @author a.kumar.samantaray
 */
public class SAPUtil {

    private SAPUtil() {
        throw new IllegalStateException("SAPUtil class cannot be initiated");
    }



    /**
     * This method helps to unescape the characters in the string according to use case.
     * 
     */
    public static String unescape(String input) {
        StringBuilder rtn = new StringBuilder(input.length());
        int i = 0;
        int end = input.length();
        while (i < end) {
            if (input.charAt(i) == '_') {
                i++;
                if (input.charAt(i) == '-') {
                    i++;
                    if (input.charAt(i) == '-') {
                        try {
                            rtn.append((char) Codecs.Hex.decode(input.charAt(i + 1), input.charAt(i + 2)));
                            i += 3;
                        }
                        catch (Exception e) {
                            throw new ConnectorException("Illegal escape sequence _--" + input.charAt(i + 1)
                                    + input.charAt(i + 2) + " encountered", e);
                        }

                    }

                    rtn.append('/');
                    continue;
                }

                rtn.append('_');
                continue;
            }

            rtn.append(input.charAt(i));
            i++;
        }

        return rtn.toString();
    }

    /**
     * This method helps to escape/append the characters from the string according to use case.
     * 
     */
    public static final String escape(String name) {
        if( StringUtil.isEmpty(name)) {
            return name;
        }
        StringBuilder buf = new StringBuilder(name.length() + 6);
        char ch1 = name.charAt(0);
        if ((Character.isLetter(ch1)) || (ch1 == '_')) {
            buf.append(ch1);
        }
        else if (ch1 == '/') {
            buf.append("_-");
        }
        else {
            buf.append("_--");
            buf.append(Codecs.Hex.encode((byte) ch1));
        }

        for (int i = 1; i < name.length(); i++) {
            char ch = name.charAt(i);
            if ((Character.isLetterOrDigit(ch)) || (ch == '_')) {
                buf.append(ch);
            }
            else if (ch == '/') {
                buf.append("_-");
            }
            else {
                buf.append("_--");
                buf.append(Codecs.Hex.encode((byte) ch));
            }
        }
        return buf.toString();
    }


    /**
     * This method helps to convert the Date received from SAP system to desired format.
     * 
     */
	public static String formatSAPDate(Date creationDate) {
		String formattedDate = "";
		try {
			if(creationDate != null) {
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(SAPJcoConstants.DATEPATTERN);
				formattedDate = simpleDateFormat.format(creationDate);
			}
		}catch(Exception e) {
			//ignoring in case of exception and sending the blank string.
		}
		return formattedDate;
	}


	/**
     * This method helps to convert the Time received from SAP system to desired format.
     * 
     */
	public static String formatSAPTime(Date creationTime) {
		String formattedTime = "";
		try {
			if(creationTime != null) {
				SimpleDateFormat format = new SimpleDateFormat(SAPJcoConstants.TIMEPATTERN);
				formattedTime = format.format(creationTime );
			}
		}catch(Exception e) {
			//empty catch block
		}
		return formattedTime;
	}    
	
	/**
	 * This method will set the value to properties only if the value is not null and not empty.
	 */
	public static void setIfNotBlank(Properties props, String property, String value) {
		if(StringUtils.isNotBlank(value)) {
			props.setProperty(property, value);
		}
	}
}
