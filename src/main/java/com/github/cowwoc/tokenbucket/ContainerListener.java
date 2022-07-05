package com.github.cowwoc.tokenbucket;

import java.time.Instant;
import java.util.List;

/**
 * Listens for container events.
 * <p>
 * Containers are notified of descendant events, before the descendants.
 * The listener is invoked while holding a lock, so special care must be taken to avoid deadlocks.
 */
public interface ContainerListener
{
	/**
	 * Invoked before sleeping to wait for more tokens.
	 *
	 * @param container   the container the thread is waiting on
	 * @param tokens      the number of tokens that the thread is waiting for
	 * @param requestedAt the time at which the tokens were requested
	 * @param availableAt the time at which the requested tokens are expected to become available
	 * @param bottlenecks the list of Limits that are preventing tokens from being consumed
	 * @throws InterruptedException if the operation should be interrupted
	 */
	default void beforeSleep(Container container, long tokens, Instant requestedAt, Instant availableAt,
	                         List<Limit> bottlenecks)
		throws InterruptedException
	{
	}
}