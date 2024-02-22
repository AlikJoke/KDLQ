# KDLQ

A library implements functionality for Kafka redelivery to reprocess messages and send messages to a separate dead letter queue (DLQ). 
Library provides 4 basic abstractions:
1. ```ru.joke.kdlq.KDLQConfiguration```: configuration that sets all necessary settings for the library's operation: 
broker servers, DLQ topic name for erroneous messages, optional topic name for redelivery, maximum number of retries for faulty messages in DLQ, 
maximum number of redeliveries before sending to DLQ, and an optional set of message lifecycle listeners.
2. ```ru.joke.kdlq.KDLQMessageProcessor```: abstraction of a user message handler with application logic; 
the implementation is always provided by the client of the library. The handler should either throw an exception or return one of the message processing statuses.
3. ```ru.joke.kdlq.KDLQMessageConsumer```: Kafka message consumer that takes actions on messages after processing, such as redelivery, sending to DLQ, 
or doing nothing if processing was successful. If a fatal error occurs during redelivery or sending to DLQ related to the broker or connection, 
the consumer throws a ```ru.joke.kdlq.KDLQException```.
4. ```ru.joke.kdlq.KDLQMessageLifecycleListener```: a message lifecycle listener within the library, containing hooks for each of the main actions. Should be registered in the configuration.

## Message redelivery for reprocessing
Message redelivery can be used if an error occurred during message processing that may resolve over time. 
For example, if message processing depends on data replication and the data has not been replicated yet, 
but the replication will occur shortly, and the message will be successfully processed.

Redelivery settings include the ability to configure the redelivery queue (if not specified, the original queue from which the message was obtained will be used) 
and the number of redelivery attempts (before the message is sent to DLQ). Redelivery is activated if the application handler 
throws a special exception (```ru.joke.kdlq.KDLQMessageMustBeRedeliveredException```) or returns ```ru.joke.kdlq.KDLQMessageProcessor.ProcessingStatus#MUST_BE_REDELIVERED``` 
during processing. Infinite redelivery is possible if the number of redelivery attempts is set to a negative value (```-1```, for example).

## Sending erroneous messages to DLQ
Sending messages to the DLQ occurs if the application handler throws any exception (except ```ru.joke.kdlq.KDLQMessageMustBeRedeliveredException```) 
or returns ```ru.joke.kdlq.KDLQMessageProcessor.ProcessingStatus#ERROR``` during processing. Sending to DLQ occurs in the queue specified in the configuration. 
The configuration also sets the maximum number of message sends to DLQ. For example, if a message has been sent to DLQ once, and then the message is reprocessed 
from DLQ, and the maximum number of message sends to DLQ is set to ```1```, in case of a repeat error, the message will be ignored and skipped, not sent back to DLQ.

## Informational and marker message headers
If a message has been redelivered or sent to DLQ, the library adds a redelivery counter (```KDLQ_Redelivered``` / ```KDLQ_Kills```) to the headers of that message. 
Additionally, a ```KDLQ_PrcMarker``` header is added, containing a handler ID marker (maybe required for further message filtering in the handler code).
Optionally, depending on the configuration (```ru.joke.kdlq.KDLQConfiguration.addOptionalInformationalHeaders```), to headers can be added (from the original message):
1. Offset (```KDLQ_OrigOffset```)
2. Partition (```KDLQ_OrigPartition```)
3. Timestamp (```KDLQ_OrigTs```).

## Examples of usage
For examples of library usage, see the Java-doc for ```ru.joke.kdlq.impl.ConfigurableKDLQMessageConsumer```, as well as in tests.
