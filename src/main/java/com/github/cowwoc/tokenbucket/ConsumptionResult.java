package com.github.cowwoc.tokenbucket;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * The result of a request to consume tokens.
 */
@SuppressWarnings("ClassCanBeRecord")
public final class ConsumptionResult
{
	private final Container container;
	private final long minimumTokensRequested;
	private final long maximumTokensRequested;
	private final long tokensConsumed;
	private final Instant requestedAt;
	private final Instant availableAt;
	private final List<Limit> bottlenecks;

	/**
	 * Creates a result of a request to consume tokens.
	 *
	 * @param container              the container that gave up tokens
	 * @param minimumTokensRequested the minimum number of tokens that were requested (inclusive)
	 * @param maximumTokensRequested the maximum number of tokens that were requested (inclusive)
	 * @param tokensConsumed         the number of tokens that were consumed
	 * @param requestedAt            the time at which the tokens were requested
	 * @param availableAt            the time at which the requested tokens are expected to become available.
	 *                               If tokens were consumed, this value is equal to {@code requestedAt}.
	 * @param bottlenecks            the list of Limits that are preventing tokens from being consumed (empty if
	 *                               none)
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code minimumTokensRequested} or {@code maximumTokensRequests}
	 *                                  are negative or zero. If {@code tokensConsumed} is negative. If
	 *                                  {@code minimumTokensRequested > maximumTokensRequested}. If
	 *                                  {@code tokensConsumed > 0 && tokensConsumed < minimumTokensRequested}.
	 */
	public ConsumptionResult(Container container, long minimumTokensRequested, long maximumTokensRequested,
	                         long tokensConsumed, Instant requestedAt, Instant availableAt,
	                         List<Limit> bottlenecks)
	{
		assertThat(container, "container").isNotNull();
		assertThat(minimumTokensRequested, "minimumTokensRequested").isPositive();
		assertThat(maximumTokensRequested, "maximumTokensRequested").isPositive().
			isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
		assertThat(tokensConsumed, "tokensConsumed").isNotNegative();
		if (tokensConsumed > 0)
		{
			requireThat(tokensConsumed, "tokensConsumed").
				isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
		}
		assertThat(requestedAt, "requestedAt").isNotNull();
		assertThat(availableAt, "availableAt").isNotNull();
		assertThat(bottlenecks, "bottlenecks").isNotNull();
		this.container = container;
		this.minimumTokensRequested = minimumTokensRequested;
		this.maximumTokensRequested = maximumTokensRequested;
		this.tokensConsumed = tokensConsumed;
		this.requestedAt = requestedAt;
		this.availableAt = availableAt;
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
	 * this value is equal to {@code requestedAt}.
	 *
	 * @return the time at which the requested tokens are expected to become available
	 */
	public Instant getAvailableAt()
	{
		return availableAt;
	}

	/**
	 * Returns the amount of time until the requested number of tokens will become available.
	 *
	 * @return the amount of time until the requested number of tokens will become available
	 */
	public Duration getAvailableIn()
	{
		return Duration.between(requestedAt, availableAt);
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
			other.requestedAt.equals(requestedAt) && other.availableAt.equals(availableAt) &&
			other.getBottlenecks().equals(bottlenecks);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(container, minimumTokensRequested, maximumTokensRequested, tokensConsumed,
			requestedAt, availableAt, bottlenecks);
	}

	@Override
	public String toString()
	{
		StringJoiner properties = new StringJoiner(",\n");
		properties.add("successful: " + isSuccessful());
		properties.add("tokensConsumed: " + tokensConsumed);
		properties.add("minimumTokensRequested: " + minimumTokensRequested);
		properties.add("maximumTokensRequested: " + maximumTokensRequested);
		properties.add("requestedAt: " + requestedAt);
		properties.add("availableAt: " + availableAt);

		StringJoiner bottlenecksJoiner = new StringJoiner(", ");
		for (Limit bottlenecks : bottlenecks)
			bottlenecksJoiner.add(bottlenecks.toString());

		properties.add("bottlenecks: " + bottlenecksJoiner);
		properties.add("container: " + container);
		return "\n" +
			"[\n" +
			"\t" + properties.toString().replaceAll("\n", "\n\t") + "\n" +
			"]";
	}
}