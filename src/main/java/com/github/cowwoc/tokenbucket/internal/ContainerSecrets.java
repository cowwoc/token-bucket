package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.tokenbucket.ConsumptionResult;
import com.github.cowwoc.tokenbucket.Limit;
import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;

import java.time.Instant;
import java.util.List;

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
	 * @implNote This method acquires its own locks
	 */
	long getAvailableTokens(AbstractContainer container);

	/**
	 * Returns the list of limits with less than the specified number of tokens.
	 *
	 * @param container the container
	 * @param tokens    the minimum number of tokens to consume
	 * @return the list of limits with less than the specified number of tokens
	 * @implNote This method acquires its own locks
	 */
	List<Limit> getLimitsWithInsufficientTokens(AbstractContainer container, long tokens);

	/**
	 * Returns the maximum number of tokens that this container can ever hold.
	 *
	 * @param container the container
	 * @return the maximum number of tokens that this container can ever hold
	 * @implNote This method acquires its own locks
	 */
	long getMaximumTokens(AbstractContainer container);

	/**
	 * Consumes {@code [minimumTokens, maximumTokens]} tokens, only if they are available at the time of
	 * invocation. Consumption order is not guaranteed to be fair.
	 *
	 * @param container           the container
	 * @param minimumTokens       the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens       the maximum  number of tokens to consume (inclusive)
	 * @param nameOfMinimumTokens the name of the {@code minimumTokens} parameter
	 * @param requestedAt         the time at which the tokens were requested
	 * @param consumedAt          the time at which an attempt was made to consume tokens
	 * @return the result of the operation
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code nameOfMinimumTokens} is empty. If
	 *                                  {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maximumTokens} that is less than {@code minimumTokens}. If
	 *                                  {@code requestedAt > consumedAt}.
	 * @implNote This method acquires its own locks. Callers are responsible for validating all parameters.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(AbstractContainer container, long minimumTokens, long maximumTokens,
	                             String nameOfMinimumTokens, Instant requestedAt, Instant consumedAt);

	/**
	 * Sets the parent of a container.
	 *
	 * @param child  the child container
	 * @param parent the parent container ({@code null} if the child has no parent)
	 * @throws NullPointerException     if {@code child} is null
	 * @throws IllegalArgumentException if setting the parent would introduce a loop in the hierarchy
	 * @implNote This method acquires its own locks
	 */
	void setParent(AbstractContainer child, AbstractContainer parent);
}