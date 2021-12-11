package org.phial.zkclient.exception;

public class ZkAuthFailedException extends ZkException {

    private static final long serialVersionUID = 1L;

    public ZkAuthFailedException() {
        super();
    }

    public ZkAuthFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZkAuthFailedException(String message) {
        super(message);
    }

    public ZkAuthFailedException(Throwable cause) {
        super(cause);
    }
}