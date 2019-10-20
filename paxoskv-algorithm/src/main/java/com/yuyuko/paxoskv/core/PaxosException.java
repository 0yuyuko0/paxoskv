package com.yuyuko.paxoskv.core;

public class PaxosException extends RuntimeException {
    public PaxosException() {
    }

    public PaxosException(String message) {
        super(message);
    }

    public PaxosException(String message, Throwable cause) {
        super(message, cause);
    }

    public PaxosException(Throwable cause) {
        super(cause);
    }

    public PaxosException(String message, Throwable cause, boolean enableSuppression,
                          boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
