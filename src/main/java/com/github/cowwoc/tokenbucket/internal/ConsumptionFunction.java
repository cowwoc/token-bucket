package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.tokenbucket.ConsumptionResult;
import com.github.cowwoc.tokenbucket.Limit;
import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;

import java.time.Instant;

/**
 * Consumes tokens from a container.
 */
public interface ConsumptionFunction
{
	/**
	 * Consumes {@code [minimumTokens, maximumTokens]} tokens, only if they are available at the time of
	 * invocation. Consumption order is not guaranteed to be fair.
	 *
	 * @param minimumTokens       the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens       the maximum number of tokens to consume (inclusive)
	 * @param nameOfMinimumTokens the name of the {@code minimumTokens} parameter
	 * @param requestedAt         the time at which the tokens were requested
	 * @param consumedAt          the time at which an attempt was made to consume tokens
	 * @param container           the enclosing container
	 * @return the result of the operation
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@link Limit#getMaximumTokens() maximumTokens} that is less than
	 *                                  {@code minimumTokens}. If {@code requestedAt > consumedAt}.
	 * @implNote This method acquires its own locks. Callers are responsible for validating all parameters.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long minimumTokens, long maximumTokens, String nameOfMinimumTokens,
	                             Instant requestedAt, Instant consumedAt, AbstractContainer container);
}