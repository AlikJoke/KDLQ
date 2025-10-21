/**
 * Contains the SPI for enabling delayed message redelivery via KDLQ. SPI implementations can
 * utilize various means as message storage and for distributed lock management.
 *
 * @see ru.joke.kdlq.spi.KDLQGlobalDistributedLockService
 * @see ru.joke.kdlq.spi.KDLQRedeliveryStorage
 * @see ru.joke.kdlq.KDLQGlobalConfiguration
 */
package ru.joke.kdlq.spi;