package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;

import java.util.concurrent.TimeUnit;

/**
 * A container of tokens.
 */
public interface Container
{
	/**
	 * Returns the data associated with this container.
	 *
	 * @return the data associated with this container
	 */
	Object getUserData();

	/**
	 * Attempts to consume a single token.
	 *
	 * @return the result of the operation
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume();

	/**
	 * Attempts to consume exactly {@code tokens}. Consumption is not guaranteed to be fair.
	 *
	 * @param tokens the number of tokens to consume
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code tokens} is negative. If one of the limits has
	 *                                  {@code maxTokens} that is less than {@code tokens}.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long tokens);

	/**
	 * Attempts to consume {@code [minimumTokens, maximumTokens]}. Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maxTokens} that is less than {@code minimumTokensRequested}.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsumeRange(long minimumTokens, long maximumTokens);

	/**
	 * Consumes a single token.
	 *
	 * @return the result of the operation
	 * @throws InterruptedException if the thread is interrupted while waiting for tokens to become available
	 */
	@CheckReturnValue
	ConsumptionResult consume() throws InterruptedException;

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
	ConsumptionResult consume(long tokens) throws InterruptedException;

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
	ConsumptionResult consume(long tokens, long timeout, TimeUnit unit) throws InterruptedException;

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
	ConsumptionResult consumeRange(long minimumTokens, long maximumTokens) throws InterruptedException;

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
	ConsumptionResult consumeRange(long minimumTokens, long maximumTokens, long timeout, TimeUnit unit)
		throws InterruptedException;
}