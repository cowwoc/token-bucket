package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.internal.ToStringBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static com.github.cowwoc.requirements.DefaultRequirements.assertionsAreEnabled;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * The result of an attempt to consume tokens.
 */
public final class ConsumptionResult
{
	private final Container container;
	private final long minimumTokensRequested;
	private final long maximumTokensRequested;
	private final long tokensConsumed;
	private final Instant requestedAt;
	private final Instant consumedAt;
	private final Instant availableAt;
	private final long tokensLeft;
	private final List<Limit> bottlenecks;

	/**
	 * Creates a result of a request to consume tokens.
	 *
	 * @param container              the container that gave up tokens
	 * @param minimumTokensRequested the minimum number of tokens that were requested (inclusive)
	 * @param maximumTokensRequested the maximum number of tokens that were requested (inclusive)
	 * @param tokensConsumed         the number of tokens that were consumed
	 * @param requestedAt            the time at which the tokens were requested
	 * @param consumedAt             the time at which an attempt was made to consume tokens. This value
	 *                               differs from {@code requestedAt} in that {@code consumedAt} is set
	 *                               after acquiring a write-lock.
	 * @param availableAt            the time at which the requested tokens are expected to become available.
	 *                               If tokens were consumed, this value is equal to {@code consumedAt}.
	 * @param tokensLeft             the number of tokens left
	 * @param bottlenecks            the list of Limits that are preventing tokens from being consumed (empty if
	 *                               none)
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code minimumTokensRequested} or {@code maximumTokensRequests}
	 *                                  are negative or zero. If {@code tokensConsumed} or {@code tokensLeft}
	 *                                  are negative. If
	 *                                  {@code minimumTokensRequested > maximumTokensRequested}. If
	 *                                  {@code tokensConsumed > 0 && tokensConsumed < minimumTokensRequested}.
	 *                                  If {@code requestedAt > consumedAt} or {@code consumedAt > availableAt}.
	 */
	public ConsumptionResult(Container container, long minimumTokensRequested, long maximumTokensRequested,
	                         long tokensConsumed, Instant requestedAt, Instant consumedAt, Instant availableAt,
	                         long tokensLeft, List<Limit> bottlenecks)
	{
		if (assertionsAreEnabled())
		{
			requireThat(container, "container").isNotNull();
			requireThat(minimumTokensRequested, "minimumTokensRequested").isPositive();
			requireThat(maximumTokensRequested, "maximumTokensRequested").isPositive().
				isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
			requireThat(tokensConsumed, "tokensConsumed").isNotNegative();
			if (tokensConsumed > 0)
			{
				requireThat(tokensConsumed, "tokensConsumed").
					isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
			}
			requireThat(requestedAt, "requestedAt").isNotNull();
			requireThat(consumedAt, "consumedAt").isGreaterThanOrEqualTo(requestedAt, "requestedAt");
			requireThat(availableAt, "availableAt").isGreaterThanOrEqualTo(consumedAt, "consumedAt");
			requireThat(tokensLeft, "tokensLeft").isNotNegative();
			requireThat(bottlenecks, "bottlenecks").isNotNull();
		}
		this.container = container;
		this.minimumTokensRequested = minimumTokensRequested;
		this.maximumTokensRequested = maximumTokensRequested;
		this.tokensConsumed = tokensConsumed;
		this.requestedAt = requestedAt;
		this.consumedAt = consumedAt;
		this.availableAt = availableAt;
		this.tokensLeft = tokensLeft;
		this.bottlenecks = bottlenecks;
	}

	/**
	 * Returns the container that tokens were consumed from. If tokens were consumed from a single bucket, it is
	 * returned. If tokens were consumed from multiple buckets, the lowest common ancestor is returned.
	 *
	 * @return the container that gave up tokens
	 */
	public Container getContainer()
	{
		return container;
	}

	/**
	 * Returns the minimum number of tokens that were requested.
	 *
	 * @return the minimum number of tokens that were requested (inclusive)
	 */
	public long getMinimumTokensRequested()
	{
		return minimumTokensRequested;
	}

	/**
	 * Returns the maximum number of tokens that were requested.
	 *
	 * @return the maximum number of tokens that were requested (inclusive)
	 */
	public long getMaximumTokensRequested()
	{
		return maximumTokensRequested;
	}

	/**
	 * Returns the number of tokens that were consumed by the request.
	 *
	 * @return the number of tokens that were consumed by the request
	 */
	public long getTokensConsumed()
	{
		return tokensConsumed;
	}

	/**
	 * Returns true if the bucket was able to consume any tokens.
	 *
	 * @return true if the bucket was able to consume any tokens
	 */
	public boolean isSuccessful()
	{
		return tokensConsumed > 0;
	}

	/**
	 * Returns the time at which the requested tokens are expected to become available. If tokens were consumed,
	 * this value is equal to {@code consumedAt}.
	 *
	 * @return the time at which the requested tokens are expected to become available
	 */
	public Instant getAvailableAt()
	{
		return availableAt;
	}

	/**
	 * Returns the time at which an attempt was made to consume tokens.
	 * <p>
	 * This value differs from {@code requestedAt} in that {@code consumedAt} is set after acquiring a
	 * write-lock.
	 *
	 * @return the time at which an attempt was made to consume tokens
	 */
	public Instant getConsumeAt()
	{
		return consumedAt;
	}

	/**
	 * Returns the amount of time until the requested number of tokens will become available.
	 *
	 * @return the amount of time until the requested number of tokens will become available
	 */
	public Duration getAvailableIn()
	{
		return Duration.between(consumedAt, availableAt);
	}

	/**
	 * Returns the number of tokens left.
	 *
	 * @return the number of tokens left
	 */
	public long getTokensLeft()
	{
		return tokensLeft;
	}

	/**
	 * Returns the list of limits that are preventing tokens from being consumed.
	 *
	 * @return empty if tokens were consumed
	 */
	public List<Limit> getBottlenecks()
	{
		return bottlenecks;
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof ConsumptionResult other))
			return false;
		return other.container == container && other.minimumTokensRequested == minimumTokensRequested &&
			other.maximumTokensRequested == maximumTokensRequested && other.tokensConsumed == tokensConsumed &&
			other.requestedAt.equals(requestedAt) && other.consumedAt.equals(consumedAt) &&
			other.availableAt.equals(availableAt) && other.tokensLeft == tokensLeft &&
			other.getBottlenecks().equals(bottlenecks);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(container, minimumTokensRequested, maximumTokensRequested, tokensConsumed,
			requestedAt, consumedAt, availableAt, tokensLeft, bottlenecks);
	}

	@Override
	public String toString()
	{
		return new ToStringBuilder(ConsumptionResult.class).
			add("successful", isSuccessful()).
			add("tokensConsumed", tokensConsumed).
			add("minimumTokensRequested", minimumTokensRequested).
			add("maximumTokensRequested", maximumTokensRequested).
			add("requestedAt", requestedAt).
			add("consumedAt", consumedAt).
			add("availableAt", availableAt).
			add("tokensLeft", tokensLeft).
			add("bottlenecks", bottlenecks).
			add("container", container).
			toString();
	}
}