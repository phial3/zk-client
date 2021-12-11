
package org.phial.zkclient.exception;

public class ZkInterruptedException extends ZkException {

    private static final long serialVersionUID = 1L;

    public ZkInterruptedException(InterruptedException e) {
        super(e);
        Thread.currentThread().interrupt();
    }
}
