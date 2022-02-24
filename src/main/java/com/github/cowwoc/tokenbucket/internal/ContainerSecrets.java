package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.ConsumptionResult;

import java.time.Instant;

/**
 * @see SharedSecrets
 */
public interface ContainerSecrets
{
	/**
	 * Returns the number of tokens that are available, without triggering a refill.
	 *
	 * @param container the container
	 * @return the number of tokens that are available
	 */
	long getAvailableTokens(AbstractContainer container);

	/**
	 * Returns the maximum number of tokens that this container can ever hold.
	 *
	 * @param container the container
	 * @return the maximum number of tokens that this container can ever hold
	 */
	long getMaximumTokens(AbstractContainer container);

	/**
	 * Attempts to consume {@code [minimumTokens, maximumTokens]}. Consumption is not guaranteed to be fair.
	 *
	 * @param container     the container
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum  number of tokens to consume (inclusive)
	 * @param requestedAt   the time at which the tokens were requested
	 * @return the minimum amount of time until the requested number of tokens will be available
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code nameOfMinimumTokens} is empty. If
	 *                                  {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maximumTokens} that is less than {@code minimumTokens}.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(AbstractContainer container, long minimumTokens, long maximumTokens,
	                             String nameOfMinimumTokens, Instant requestedAt);
}