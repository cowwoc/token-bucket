/**
 * <h1>Locking policy</h1>
 * <p>
 * Unless otherwise stated, public methods are responsible for acquiring locks on behalf of non-public
 * methods that they invoke.
 * <p>
 * As of JDK 19, {@link java.util.concurrent.locks.StampedLock} outperforms every other type of lock type,
 * but does not support {@link java.util.concurrent.locks.Condition} variables. See the following
 * <a href="https://vimeo.com/74553130">video</a> and
 * <a href="https://www.javaspecialists.eu/talks/jfokus13/PhaserAndStampedLock.pdf">article</a>.
 * <p>
 * We use {@code StampedLock} for thread-safety and {@code ReadWriteLock} to signal/await {@code Condition}
 * variables.
 */
package com.github.cowwoc.tokenbucket.internal;