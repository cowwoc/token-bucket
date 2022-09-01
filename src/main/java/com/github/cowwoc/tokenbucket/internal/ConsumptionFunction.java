package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.tokenbucket.ConsumptionResult;
import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;

import java.time.Instant;

/**
 * Consumes tokens from a bucket.
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
	 * @param bucket              the enclosing bucket
	 * @return the result of the operation
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maximumTokens} that is less than {@code minimumTokensRequested}.
	 *                                  If {@code requestedAt > consumedAt}
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long minimumTokens, long maximumTokens, String nameOfMinimumTokens,
	                             Instant requestedAt, Instant consumedAt, AbstractContainer bucket);
}