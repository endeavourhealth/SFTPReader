package org.endeavourhealth.sftpreader.model.exceptions;

public class SftpValidationException extends SftpReaderException {
    static final long serialVersionUID = 0L;

    public SftpValidationException() {
        super();
    }
    public SftpValidationException(String message) {
        super(message);
    }
    public SftpValidationException(String message, Throwable cause) {
        super(message, cause);
    }
    public SftpValidationException(Throwable cause) {
        super(cause);
    }
}
