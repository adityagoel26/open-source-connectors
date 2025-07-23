//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.liveoptics;

/**
 * @author Sudeshna Bhattacharjee
 *
 * ${tags}
 */

@SuppressWarnings("serial")
public class ArgumentException extends Exception {
	
         public ArgumentException(String s) {
        	 super(s);
         }
         
         public ArgumentException(String s,Exception ex) {
        	 super(s, ex);
         }
}
