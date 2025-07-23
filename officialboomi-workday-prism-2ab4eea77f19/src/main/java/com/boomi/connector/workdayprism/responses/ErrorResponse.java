//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
/**
 * Class modeling the error response
 *
 * @author saurav.b.sengupta <saurav.b.sengupta@accenture.com>
 */
public class ErrorResponse implements Serializable {
    private static final long serialVersionUID = -201808289854L;

    private String error;
    private List<ErrorBody> errors;

    @JsonCreator
    public ErrorResponse(@JsonProperty("error") String error, @JsonProperty("errors") List<ErrorBody> errors) {
        this.error = error;
		this.errors = errors;
    }

    public String getError() {
        return error;
    }
     
    
    public List<ErrorBody> getErrors() {
		return errors;
	}

	
	private static class ErrorBody implements Serializable {
        private static final long serialVersionUID = -20180828910L;
        
        private String error;
        
        @JsonCreator
        public ErrorBody(@JsonProperty("error") String error) {
            this.error = error;
        }
        
        public String getError() {
            return error;
        }
        
    }
    
}
