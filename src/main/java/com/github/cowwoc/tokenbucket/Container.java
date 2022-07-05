package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A container of tokens.
 */
public interface Container
{
	/**
	 * Returns the listeners associated with this container.
	 *
	 * @return an unmodifiable list
	 */
	List<ContainerListener> getListeners();

	/**
	 * Returns the data associated with this container.
	 *
	 * @return the data associated with this container
	 */
	Object getUserData();

	/**
	 * Consumes a single token, only if one is available at the time of invocation. Consumption order is not
	 * guaranteed to be fair.
	 *
	 * @return the result of the operation
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume();

	/**
	 * Consumes {@code tokens} tokens, only if they are available at the time of invocation. Consumption
	 * order is not guaranteed to be fair.
	 *
	 * @param tokens the number of tokens to consume
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero. If the request can never
	 *                                  succeed because the container cannot hold the requested number of
	 *                                  tokens.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long tokens);

	/**
	 * Consumes the requested number of {@code tokens}, only if they become available within the given waiting
	 * time. Consumption order is not guaranteed to be fair.
	 *
	 * @param tokens  the number of tokens to consume
	 * @param timeout the maximum amount of time to wait
	 * @param unit    the unit of {@code timeout}
	 * @return the result of the operation
	 * @throws NullPointerException     if {@code unit} is null
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero. If {@code timeout} is negative.
	 *                                  If the request can never succeed because the container cannot hold the
	 *                                  requested number of tokens.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long tokens, long timeout, TimeUnit unit) throws InterruptedException;

	/**
	 * Consumes {@code [minimumTokens, maximumTokens]} tokens, only if they are available at the time of
	 * invocation. Consumption order is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @return the result of the operation
	 * @throws IllegalArgumentException if the arguments are negative or zero. If
	 *                                  {@code minimumTokens > maximumTokens}. If the request can never
	 *                                  succeed because the container cannot hold the requested number of
	 *                                  tokens.
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long minimumTokens, long maximumTokens);

	/**
	 * Consumes {@code [minimumTokens, maximumTokens]} tokens, only if they become available within the
	 * given waiting time. Consumption order is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @param timeout       the maximum amount of time to wait
	 * @param unit          the unit of {@code timeout}
	 * @return the result of the operation
	 * @throws NullPointerException     if {@code unit} is null
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero. If {@code timeout} is negative.
	 *                                  If {@code minimumTokens > maximumTokens}. If the request can never
	 *                                  succeed because the container cannot hold the requested number of
	 *                                  tokens.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	ConsumptionResult tryConsume(long minimumTokens, long maximumTokens, long timeout, TimeUnit unit)
		throws InterruptedException;

	/**
	 * Consumes a single token, blocking until it becomes available. Consumption order is not guaranteed to be
	 * fair.
	 *
	 * @return the result of the operation
	 * @throws InterruptedException if the thread is interrupted while waiting for tokens to become available
	 */
	@CheckReturnValue
	ConsumptionResult consume() throws InterruptedException;

	/**
	 * Consumes the specified number of tokens, blocking until they become available. Consumption order is not
	 * guaranteed to be fair.
	 *
	 * @param tokens the number of tokens to consume
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero. If the request can never
	 *                                  succeed because the container cannot hold the requested number of
	 *                                  tokens.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	ConsumptionResult consume(long tokens) throws InterruptedException;

	/**
	 * Consumes {@code [minimumTokens, maximumTokens]} tokens, blocking until they become available.
	 * Consumption order is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @return the result of the operation
	 * @throws NullPointerException     if {@code unit} is null
	 * @throws IllegalArgumentException if {@code tokens} or {@code timeout} are negative or zero. If
	 *                                  {@code minimumTokens > maximumTokens}. If the request can never
	 *                                  succeed because the container cannot hold the requested number of
	 *                                  tokens.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	ConsumptionResult consume(long minimumTokens, long maximumTokens) throws InterruptedException;
}