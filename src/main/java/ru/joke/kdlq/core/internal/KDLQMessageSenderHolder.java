package ru.joke.kdlq.core.internal;

import javax.annotation.Nonnull;

public abstract class KDLQMessageSenderHolder {

    private static final KDLQMessageSender sender = new KDLQMessageSender();

    @Nonnull
    public static KDLQMessageSender get() {
        return sender;
    }
    
    private KDLQMessageSenderHolder() {}
}
