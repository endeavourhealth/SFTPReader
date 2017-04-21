package org.endeavourhealth.sftpreader.model.exceptions;

public class SftpReaderException extends Exception {
    static final long serialVersionUID = 0L;

    public SftpReaderException() {
        super();
    }
    public SftpReaderException(String message) {
        super(message);
    }
    public SftpReaderException(String message, Throwable cause) {
        super(message, cause);
    }
    public SftpReaderException(Throwable cause) {
        super(cause);
    }
}
