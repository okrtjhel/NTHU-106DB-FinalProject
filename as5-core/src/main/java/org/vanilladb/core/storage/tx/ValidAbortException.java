package org.vanilladb.core.storage.tx;

@SuppressWarnings("serial")
public class ValidAbortException extends RuntimeException {
	public ValidAbortException() {
	}
	
	public ValidAbortException(String message) {
		super(message);
	}
}
