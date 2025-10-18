module kdlq.core {

    requires kafka.clients;
    requires org.slf4j;
    requires jsr305;

    exports ru.joke.kdlq;
    exports ru.joke.kdlq.spi;
    exports ru.joke.kdlq.internal.util to kdlq.mongo;
}