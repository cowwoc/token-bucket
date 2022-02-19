package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public abstract class AbstractBucket
{
	/**
	 * @return the mutex used to lock this bucket's state
	 */
	protected abstract Object getMutex();

	/**
	 * @return the logger associated with this bucket
	 */
	protected abstract Logger getLogger();

	/**
	 * Attempts to consume {@code [minimumTokens, maximumTokens]} . Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum  number of tokens to consume (inclusive)
	 * @param requestedAt   the time at which the user attempted to consume the tokens
	 * @return the minimum amount of time until the requested number of tokens will be available
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maxTokens} that is less than {@code minimumTokensRequested}.
	 */
	@CheckReturnValue
	protected abstract ConsumptionResult tryConsumeRange(long minimumTokens, long maximumTokens,
	                                                     Instant requestedAt);

	/**
	 * Attempts to consume a single token.
	 *
	 * @return the result of the operation
	 */
	@CheckReturnValue
	public ConsumptionResult tryConsume()
	{
		return tryConsume(1);
	}

	/**
	 * Attempts to consume exactly {@code tokens}. Consumption is not guaranteed to be fair.
	 *
	 * @param tokens the number of tokens to consume
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code tokens} is negative. If one of the limits has
	 *                                  {@code maxTokens} that is less than {@code tokens}.
	 */
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens)
	{
		requireThat(tokens, "tokens").isNotNegative();
		Instant requestedAt = Instant.now();
		return tryConsumeRange(tokens, tokens, requestedAt);
	}

	/**
	 * Attempts to consume {@code [minimumTokens, maximumTokens]} . Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maxTokens} that is less than {@code minimumTokensRequested}.
	 */
	@CheckReturnValue
	public ConsumptionResult tryConsumeRange(long minimumTokens, long maximumTokens)
	{
		Instant requestedAt = Instant.now();
		return tryConsumeRange(minimumTokens, maximumTokens, requestedAt);
	}

	/**
	 * Consumes a single token.
	 *
	 * @return the result of the operation
	 * @throws InterruptedException if the thread is interrupted while waiting for tokens to become available
	 */
	@CheckReturnValue
	public ConsumptionResult consume() throws InterruptedException
	{
		return consume(1);
	}

	/**
	 * Blocks until consume the specified number of tokens. Consumption is not guaranteed to be fair.
	 *
	 * @param tokens the number of tokens to consume
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code tokens} is negative. If one of the limits has
	 *                                  {@code maxTokens} that is less than {@code tokens}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become available
	 */
	@CheckReturnValue
	public ConsumptionResult consume(long tokens) throws InterruptedException
	{
		Instant requestedAt = Instant.now();
		return consume(tokens, tokens, requestedAt, consumptionResult -> false);
	}

	/**
	 * Blocks until to exactly {@code tokens} are consumed. Consumption is not guaranteed to be fair.
	 *
	 * @param tokens  the number of tokens to consume
	 * @param timeout the maximum amount of time to wait
	 * @param unit    the unit of {@code timeout}
	 * @return the result of the operation
	 * @throws NullPointerException     if {@code unit} is null
	 * @throws IllegalArgumentException if {@code tokens} or {@code timeout} are negative or zero. If one of
	 *                                  the bucket limits has {@code maxTokens} that is less than
	 *                                  {@code tokens}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	public ConsumptionResult consume(long tokens, long timeout, TimeUnit unit) throws InterruptedException
	{
		requireThat(timeout, "timeout").isNotNegative();
		requireThat(unit, "unit").isNotNull();
		Instant requestedAt = Instant.now();
		Instant timeLimit = requestedAt.plus(timeout, unit.toChronoUnit());
		return consume(tokens, tokens, requestedAt,
			consumptionResult -> !consumptionResult.getTokensAvailableAt().isBefore(timeLimit));
	}

	/**
	 * Blocks until {@code [minimumTokens, maximumTokens]} are consumed. Consumption is not guaranteed to
	 * be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @return the result of the operation
	 * @throws NullPointerException     if {@code unit} is null
	 * @throws IllegalArgumentException if {@code tokens} or {@code timeout} are negative or zero. If
	 *                                  {@code minimumTokens > maximumTokens}. If one of the bucket limits
	 *                                  has {@code maxTokens} that is less than {@code minimumTokens}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	public ConsumptionResult consumeRange(long minimumTokens,
	                                      long maximumTokens) throws InterruptedException
	{
		Instant requestedAt = Instant.now();
		return consume(minimumTokens, maximumTokens, requestedAt, consumptionResult -> false);
	}

	/**
	 * Blocks until {@code [minimumTokens, maximumTokens]} are consumed. Consumption is not guaranteed to
	 * be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @param timeout       the maximum amount of time to wait
	 * @param unit          the unit of {@code timeout}
	 * @return the result of the operation
	 * @throws NullPointerException     if {@code unit} is null
	 * @throws IllegalArgumentException if {@code tokens} or {@code timeout} are negative or zero. If
	 *                                  {@code minimumTokens > maximumTokens}. If one of the bucket limits has a
	 *                                  {@code maxTokens} that is less than {@code minimumTokens}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	public ConsumptionResult consumeRange(long minimumTokens, long maximumTokens, long timeout,
	                                      TimeUnit unit)
		throws InterruptedException
	{
		requireThat(timeout, "timeout").isNotNegative();
		requireThat(unit, "unit").isNotNull();
		Instant requestedAt = Instant.now();
		Instant timeLimit = requestedAt.plus(timeout, unit.toChronoUnit());
		return consume(minimumTokens, maximumTokens, requestedAt,
			consumptionResult -> !consumptionResult.getTokensAvailableAt().isBefore(timeLimit));
	}

	/**
	 * Blocks until consume the specified number of tokens. Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokensRequested the minimum number of tokens to consume (inclusive)
	 * @param maximumTokensRequested the maximum number of tokens to consume (inclusive)
	 * @param requestedAt            the time at which the user attempted to consume the tokens
	 * @param timeout                returns true if a timeout occurs
	 * @return the result of the operation
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code tokens} or {@code timeout} are negative or zero. If one of
	 *                                  the bucket limits has a {@code maxTokens} that is less than
	 *                                  {@code tokens}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	protected ConsumptionResult consume(long minimumTokensRequested, long maximumTokensRequested,
	                                    Instant requestedAt, Function<ConsumptionResult, Boolean> timeout)
		throws InterruptedException
	{
		requireThat(minimumTokensRequested, "minimumTokensRequested").isNotNegative();
		requireThat(maximumTokensRequested, "maximumTokensRequested").isNotNegative().
			isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
		requireThat(requestedAt, "requestedAt").isNotNull();
		requireThat(timeout, "timeout").isNotNull();
		Object mutex = getMutex();
		Logger log = getLogger();
		while (true)
		{
			ConsumptionResult consumptionResult = tryConsumeRange(minimumTokensRequested,
				maximumTokensRequested, requestedAt);
			if (consumptionResult.isSuccessful() || timeout.apply(consumptionResult))
				return consumptionResult;
			requestedAt = Instant.now();
			Duration timeLeft = consumptionResult.getTokensAvailableIn();
			log.debug("Sleeping {}", timeLeft);
			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (mutex)
			{
				mutex.wait(timeLeft.getSeconds(), timeLeft.getNano() / 1000);
			}
		}
	}
}