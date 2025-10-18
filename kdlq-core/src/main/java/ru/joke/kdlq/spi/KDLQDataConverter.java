package ru.joke.kdlq.spi;

public interface KDLQDataConverter<T> {

    byte[] toByteArray(T value);

    T fromByteArray(byte[] value);
}
