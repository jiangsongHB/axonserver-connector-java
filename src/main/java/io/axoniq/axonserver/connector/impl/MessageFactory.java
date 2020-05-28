package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.ErrorCode;
import io.axoniq.axonserver.grpc.ErrorMessage;

public class MessageFactory {

    private MessageFactory() {

    }

    public static ErrorMessage buildErrorMessage(ErrorCode errorCode, String client, Throwable t) {
        ErrorMessage.Builder builder = ErrorMessage.newBuilder()
                                                   .setLocation(client)
                                                   .setErrorCode(errorCode.errorCode());
        if (t != null) {
            builder.setMessage(extractMessage(t));
            builder.addDetails(extractMessage(t));
            while (t.getCause() != null) {
                t = t.getCause();
                builder.addDetails(extractMessage(t));
            }
        }
        return builder.build();
    }

    private static String extractMessage(Throwable t) {
        return t.getMessage() == null ? t.getClass().getName() : t.getMessage();
    }
}
