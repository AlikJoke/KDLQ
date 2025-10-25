package ru.joke.kdlq;

// TODO
class KDLQ_IT {
/*
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableKDLQMessageConsumerIT.class);

    private static final String DLQ_NAME = "DLQ";
    private static final String TEST_QUEUE_NAME = "TQ";

    private static final String CONSUMER_MARKER = "test";

    private static EmbeddedKafkaConfig config;
    private static EmbeddedK kafka;

    @BeforeAll
    public static void startEmbeddedKafka() {
        config = EmbeddedKafkaConfig.defaultConfig();
        kafka = EmbeddedKafka.start(config);
    }

    @AfterAll
    public static void stopEmbeddedKafka() {
        kafka.stop(true);
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        final var servers = createBootstrapServersConfig();
        final var config =
                ImmutableKDLQConfiguration
                        .builder()
                        .withProducerProperties(createProducerProperties())
                        .build(servers, DLQ_NAME);
        final var messageProcessor = new TestProcessor();

        // Fill working queue
        try (final KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(createProducerProperties())) {

            for (int i = 0; i < 9; i++) {
                producer.send(new ProducerRecord<>(TEST_QUEUE_NAME, i, String.valueOf(i).getBytes(StandardCharsets.UTF_8)), (r, e) -> {
                    if (e != null) {
                        throw new RuntimeException(e);
                    }
                }).get();
            }
        }

        // main action (consuming)
        try (final ConfigurableKDLQMessageConsumer<Integer, byte[]> consumer = new ConfigurableKDLQMessageConsumer<>(CONSUMER_MARKER, config, messageProcessor);
                final KafkaConsumer<Integer, byte[]> kafkaConsumer = new KafkaConsumer<>(createConsumerProperties())) {

            kafkaConsumer.subscribe(Set.of(TEST_QUEUE_NAME));

            final var records = kafkaConsumer.poll(Duration.ofSeconds(30));
            records.records(TEST_QUEUE_NAME)
                    .forEach(record -> logger.debug("Record processed with result: {}", consumer.accept(record)));

            kafkaConsumer.commitSync();
        }

        // checks of redelivered messages and from DLQ
        makeChecks();
    }

    private void makeChecks() {

        try (final KafkaConsumer<Integer, byte[]> kafkaConsumer = new KafkaConsumer<>(createConsumerProperties())) {
            // checks of messages from DLQ
            kafkaConsumer.subscribe(Set.of(TEST_QUEUE_NAME, DLQ_NAME));

            final var consumedRecords = kafkaConsumer.poll(Duration.ofSeconds(5));

            final var dlqRecords = consumedRecords.records(DLQ_NAME);
            assertEquals(3, StreamSupport.stream(dlqRecords.spliterator(), false).count(), "Count of sent to DLQ messages must be equal");
            dlqRecords.forEach(record -> makeMessageChecks(record, "KDLQ_Kills", 2));

            // checks of redelivered messages
            final var redeliveredRecords = consumedRecords.records(TEST_QUEUE_NAME);
            assertEquals(12, StreamSupport.stream(redeliveredRecords.spliterator(), false).count(), "Count of messages must be equal");
            final var redeliveredMessages =
                    StreamSupport.stream(redeliveredRecords.spliterator(), false)
                            .filter(record -> record.headers().lastHeader("KDLQ_Redelivered") != null)
                            .toList();
            assertEquals(3, redeliveredMessages.size(), "Count of redelivered messages must be equal");
            redeliveredMessages.forEach(record -> makeMessageChecks(record, "KDLQ_Redelivered", 1));
        }
    }

    private void makeMessageChecks(
            final ConsumerRecord<Integer, byte[]> record,
            final String counterHeaderName,
            final int messageKeyExpectedValueSurplus) {

        assertEquals(messageKeyExpectedValueSurplus, record.key() % 3, "Key of the message must satisfy to condition");
        assertArrayEquals(String.valueOf(record.key()).getBytes(StandardCharsets.UTF_8), record.value(), "Value of the message must be equal");

        assertEquals(2, record.headers().toArray().length, "Count of headers must be equal");

        final var markerHeader = record.headers().lastHeader("KDLQ_PrcMarker");
        assertNotNull(markerHeader, "Marker header must be not null");
        assertArrayEquals(CONSUMER_MARKER.getBytes(StandardCharsets.UTF_8), markerHeader.value(), "Marker header must be equal");

        final var counterHeader = record.headers().lastHeader(counterHeaderName);
        assertNotNull(counterHeader, "Counter header must be not null");
        assertEquals(1, byteArrayToInt(counterHeader.value()), "Counter header must be equal");
    }

    private int byteArrayToInt(final byte[] array) {
        int value = 0;
        for (final byte b : array) {
            value = (value << 8) + (b & 0xFF);
        }

        return value;
    }

    private Map<String, Object> createConsumerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, createBootstrapServersConfigAsString(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getCanonicalName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName(),
                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }

    private Map<String, Object> createProducerProperties() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, createBootstrapServersConfigAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getCanonicalName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName(),
                ProducerConfig.ACKS_CONFIG, "all"
        );
    }

    private String createBootstrapServersConfigAsString() {
        return String.join(",", createBootstrapServersConfig());
    }

    private Set<String> createBootstrapServersConfig() {
        return Set.of("localhost:" + config.kafkaPort());
    }

    private static class TestProcessor implements KDLQMessageProcessor<Integer, byte[]> {

        @Nonnull
        @Override
        public ProcessingStatus process(@Nonnull ConsumerRecord<Integer, byte[]> message) {
            return switch (message.key() % 3) {
                case 0 -> ProcessingStatus.OK;
                case 1 -> ProcessingStatus.MUST_BE_REDELIVERED;
                case 2 -> ProcessingStatus.ERROR;
                default -> throw new IllegalStateException("Unexpected value: " + message.key() % 3);
            };
        }
    }
 */
}
