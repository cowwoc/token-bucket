package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.ConsumptionResult;

import java.time.Instant;

/**
 * Consumes tokens from a bucket.
 */
public interface ConsumptionFunction
{
	/**
	 * Attempts to consume {@code [minimumTokens, maximumTokens]} . Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokens       the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens       the maximum number of tokens to consume (inclusive)
	 * @param nameOfMinimumTokens the name of the {@code minimumTokens} parameter
	 * @param requestedAt         the time at which the tokens were requested
	 * @param bucket              the enclosing bucket
	 * @return the minimum amount of time until the requested number of tokens will be available
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maximumTokens} that is less than {@code minimumTokensRequested}.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long minimumTokens, long maximumTokens, String nameOfMinimumTokens,
	                             Instant requestedAt, AbstractContainer bucket);
}