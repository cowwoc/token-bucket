package com.github.cowwoc.tokenbucket;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * The result of a request to consume tokens.
 */
@SuppressWarnings("ClassCanBeRecord")
public final class ConsumptionResult
{
	private final Bucket bucket;
	private final long minimumTokensRequested;
	private final long maximumTokensRequested;
	private final long tokensConsumed;
	private final long tokensLeft;
	private final Instant requestedAt;
	private final Instant tokensAvailableAt;

	/**
	 * Creates a result of a request to consume tokens.
	 *
	 * @param bucket                 the bucket associated with the result
	 * @param minimumTokensRequested the minimum number of tokens that were requested
	 * @param maximumTokensRequested the maximum number of tokens that were requested
	 * @param tokensConsumed         the number of tokens that were consumed
	 * @param tokensLeft             the number of tokens that were left for consumption
	 * @param requestedAt            the time at which the user requested to consume tokens
	 * @param tokensAvailableAt      the time at which the requested tokens will become available
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if any of the arguments are negative. If
	 *                                  {@code minimumTokensRequested > maximumTokensRequested}. If
	 *                                  {@code tokensConsumed > 0 && tokensConsumed < minimumTokensRequested}
	 */
	public ConsumptionResult(Bucket bucket, long minimumTokensRequested, long maximumTokensRequested,
	                         long tokensConsumed, long tokensLeft, Instant requestedAt,
	                         Instant tokensAvailableAt)
	{
		assertThat(bucket, "bucket").isNotNull();
		assertThat(minimumTokensRequested, "minimumTokensRequested").isNotNegative();
		assertThat(maximumTokensRequested, "maximumTokensRequested").isNotNegative().
			isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
		assertThat(tokensConsumed, "tokensConsumed").isNotNegative();
		if (tokensConsumed > 0)
		{
			requireThat(tokensConsumed, "tokensConsumed").
				isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
		}
		assertThat(tokensLeft, "tokensLeft").isNotNegative();
		assertThat(requestedAt, "requestedAt").isNotNull();
		assertThat(tokensAvailableAt, "tokensAvailableAt").isNotNull();
		this.bucket = bucket;
		this.minimumTokensRequested = minimumTokensRequested;
		this.maximumTokensRequested = maximumTokensRequested;
		this.tokensConsumed = tokensConsumed;
		this.tokensLeft = tokensLeft;
		this.requestedAt = requestedAt;
		this.tokensAvailableAt = tokensAvailableAt;
	}

	/**
	 * Returns the bucket associated with the result.
	 *
	 * @return the bucket associated with the result
	 */
	public Bucket getBucket()
	{
		return bucket;
	}

	/**
	 * Returns the minimum number of tokens that were requested.
	 *
	 * @return the minimum number of tokens that were requested
	 */
	public long getMinimumTokensRequested()
	{
		return minimumTokensRequested;
	}

	/**
	 * Returns the maximum number of tokens that were requested.
	 *
	 * @return the maximum number of tokens that were requested
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
	 * Returns the number of tokens that were left for consumption after processing the request.
	 *
	 * @return the number of tokens that were left for consumption after processing the request
	 */
	public long getTokensLeft()
	{
		return tokensLeft;
	}

	/**
	 * Indicates when the requested number of tokens will become available.
	 *
	 * @return the time when the requested number of tokens will become available
	 */
	public Instant getTokensAvailableAt()
	{
		return tokensAvailableAt;
	}

	/**
	 * Returns the amount of time until the requested number of tokens will become available.
	 *
	 * @return the amount of time until the requested number of tokens will become available
	 */
	public Duration getTokensAvailableIn()
	{
		return Duration.between(requestedAt, tokensAvailableAt);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof ConsumptionResult other))
			return false;
		return other.bucket == bucket && other.minimumTokensRequested == minimumTokensRequested &&
			other.maximumTokensRequested == maximumTokensRequested &&
			other.tokensConsumed == tokensConsumed && other.tokensLeft == tokensLeft &&
			other.requestedAt.equals(requestedAt) && other.tokensAvailableAt.equals(tokensAvailableAt);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(bucket, minimumTokensRequested, maximumTokensRequested, tokensConsumed, tokensLeft,
			requestedAt, tokensAvailableAt);
	}

	@Override
	public String toString()
	{
		return "bucket: " + bucket + ", minimumTokensRequested: " + minimumTokensRequested +
			", maximumTokensRequested: " + maximumTokensRequested + ", tokensConsumed: " + tokensConsumed +
			", tokensLeft: " + tokensLeft + ", requestedAt: " + requestedAt + ", tokensAvailableAt: " +
			tokensAvailableAt + ", isSuccessful: " + isSuccessful();
	}
}