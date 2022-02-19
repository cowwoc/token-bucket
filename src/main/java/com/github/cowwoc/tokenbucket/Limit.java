package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A rate limit.
 */
public final class Limit
{
	private long tokensPerPeriod;
	private Duration period;
	private long initialTokens;
	private long maxTokens;
	private long minimumToRefill;
	private Instant lastRefilledAt;
	private long tokensAvailable;

	/**
	 * Creates a new limit.
	 *
	 * @param bucket          the bucket associated with this limit
	 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
	 * @param period          indicates how often {@code tokensPerPeriod} should be added to the bucket
	 * @param initialTokens   the initial amount of tokens in the bucket
	 * @param maxTokens       the maximum amount of tokens that the bucket may hold before overflowing
	 *                        (subsequent tokens are discarded)
	 * @param minimumToRefill the minimum number of tokens to add to the limit
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code initialTokens > maxTokens} or
	 *                                  {@code tokensPerPeriod > maxTokens}
	 */
	Limit(long tokensPerPeriod, Duration period, long initialTokens, long maxTokens, long minimumToRefill)
	{
		// Assume that all other preconditions are enforced by Bucket.LimitAdder
		requireThat(maxTokens, "maxTokens").
			isGreaterThanOrEqualTo(tokensPerPeriod, "tokensPerPeriod").
			isGreaterThanOrEqualTo(initialTokens, "initialTokens");
		this.tokensPerPeriod = tokensPerPeriod;
		this.period = period;
		this.initialTokens = initialTokens;
		this.tokensAvailable = initialTokens;
		this.maxTokens = maxTokens;
		this.minimumToRefill = minimumToRefill;
		this.lastRefilledAt = Instant.now();
		this.tokensAvailable = initialTokens;
	}

	/**
	 * @return the amount of tokens to add to the bucket every {@code period}
	 */
	public long getTokensPerPeriod()
	{
		return tokensPerPeriod;
	}

	/**
	 * @return indicates how often {@code tokensPerPeriod} should be added to the bucket
	 */
	public Duration getPeriod()
	{
		return period;
	}

	/**
	 * @return the initial amount of tokens that the bucket starts with
	 */
	public long getInitialTokens()
	{
		return initialTokens;
	}

	/**
	 * @return the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens are discarded)
	 */
	public long getMaxTokens()
	{
		return maxTokens;
	}

	/**
	 * @return the last time at which tokens were refilled
	 */
	public Instant getLastRefilledAt()
	{
		return lastRefilledAt;
	}

	/**
	 * Sets the last time at which tokens were refilled.
	 *
	 * @param lastRefilledAt the last time at which tokens were refilled
	 */
	void setLastRefilledAt(Instant lastRefilledAt)
	{
		assertThat(lastRefilledAt, "lastRefilledAt").isNotNull();
		this.lastRefilledAt = lastRefilledAt;
	}

	/**
	 * @return the number of tokens that are available
	 */
	public long getTokensAvailable()
	{
		return tokensAvailable;
	}

	/**
	 * @return the minimum number of tokens to add to the limit
	 */
	public long getMinimumToRefill()
	{
		return minimumToRefill;
	}

	/**
	 * Refills the limit.
	 *
	 * @param requestedAt the time at which the user requested to consume tokens
	 * @throws NullPointerException if any of the arguments are null
	 */
	public void refill(Instant requestedAt)
	{
		Duration elapsed = Duration.between(lastRefilledAt, requestedAt);
		if (elapsed.isNegative())
			return;
		// The amount of time that must elapse for a single token to get added
		Duration durationPerToken = period.dividedBy(tokensPerPeriod);
		long tokensToAdd = elapsed.dividedBy(durationPerToken);
		// Fill tokens smoothly. For example, a refill rate of 60 tokens a minute will add 1 token per second
		// as opposed to 60 tokens all at once at the end of the minute.
		if (tokensToAdd < minimumToRefill)
			return;
		Duration timeElapsed = durationPerToken.multipliedBy(tokensToAdd);
		lastRefilledAt = lastRefilledAt.plus(timeElapsed);

		tokensAvailable = Math.min(maxTokens, saturatedAdd(tokensAvailable, tokensToAdd));
	}

	/**
	 * Consumes tokens.
	 *
	 * @param tokens the number of tokens
	 * @throws IllegalArgumentException if {@code tokens > tokensAvailable}
	 */
	public void consume(long tokens)
	{
		requireThat(tokens, "tokens").isLessThanOrEqualTo(tokensAvailable, "tokensAvailable");
		tokensAvailable -= tokens;
	}

	/**
	 * Adds two numbers, returning {@code Long.MAX_VALUE} or {@code Long.MIN_VALUE} if the sum would
	 * overflow or underflow, respectively.
	 *
	 * @param first  the first number
	 * @param second the second number
	 * @return the sum
	 */
	private static long saturatedAdd(long first, long second)
	{
		long result = first + second;
		if (second >= 0)
		{
			if (result >= first)
				return result;
			return Long.MAX_VALUE;
		}
		if (result < first)
			return result;
		return Long.MIN_VALUE;
	}

	/**
	 * Updates this Limit's configuration.
	 * <p>
	 * Please note that users are allowed to consume tokens between the time this method is invoked and
	 * {@link ConfigurationUpdater#apply()} completes. Users who wish to add/remove a relative amount of
	 * tokens should avoid accessing the bucket until the configuration update is complete.
	 *
	 * @return the Limit configuration
	 */
	public ConfigurationUpdater updateConfiguration()
	{
		return new ConfigurationUpdater();
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(tokensPerPeriod, period, initialTokens, maxTokens, minimumToRefill);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof Limit other)) return false;
		return tokensPerPeriod == other.tokensPerPeriod && initialTokens == other.initialTokens &&
			maxTokens == other.maxTokens && period.equals(other.period) && minimumToRefill == other.minimumToRefill;
	}

	@Override
	public String toString()
	{
		return "tokensAvailable: " + tokensAvailable + ", lastRefilledAt: " + lastRefilledAt +
			", tokensPerPeriod: " + tokensPerPeriod + ", period: " + period + ", initialTokens: " +
			initialTokens + ", maxTokens: " + maxTokens + ", minimumToRefill: " + minimumToRefill;
	}

	/**
	 * Updates this Limit's configuration.
	 */
	public class ConfigurationUpdater
	{
		private long tokensPerPeriod;
		private Duration period;
		private long initialTokens;
		private long maxTokens;
		private long minimumToRefill;
		private Instant lastRefilledAt;
		private long tokensAvailable;

		/**
		 * Creates a new configuration updater.
		 */
		private ConfigurationUpdater()
		{
			this.tokensPerPeriod = Limit.this.tokensPerPeriod;
			this.period = Limit.this.period;
			this.initialTokens = Limit.this.initialTokens;
			this.maxTokens = Limit.this.maxTokens;
			this.minimumToRefill = Limit.this.minimumToRefill;
			this.lastRefilledAt = Limit.this.lastRefilledAt;
			this.tokensAvailable = Limit.this.tokensAvailable;
		}

		/**
		 * Sets the amount of tokens to add to the bucket every {@code period}.
		 *
		 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
		 * @return this
		 */
		@CheckReturnValue
		public ConfigurationUpdater tokensPerPeriod(long tokensPerPeriod)
		{
			requireThat(tokensPerPeriod, "tokensPerPeriod").isPositive();
			this.tokensPerPeriod = tokensPerPeriod;
			return this;
		}

		/**
		 * Indicates how often {@code tokensPerPeriod} should be added to the bucket.
		 *
		 * @param period indicates how often {@code tokensPerPeriod} should be added to the bucket
		 * @return this
		 */
		@CheckReturnValue
		public ConfigurationUpdater period(Duration period)
		{
			requireThat(period, "period").isGreaterThan(Duration.ZERO);
			this.period = period;
			return this;
		}

		/**
		 * Sets the initial amount of tokens in the bucket.
		 *
		 * @param initialTokens the initial amount of tokens in the bucket
		 * @return this
		 * @throws IllegalArgumentException if {@code initialTokens} is negative
		 */
		@CheckReturnValue
		public ConfigurationUpdater initialTokens(long initialTokens)
		{
			requireThat(initialTokens, "initialTokens").isNotNegative();
			this.initialTokens = initialTokens;
			return this;
		}

		/**
		 * Sets the maximum amount of tokens that the bucket may hold before overflowing.
		 *
		 * @param maxTokens the maximum amount of tokens that the bucket may hold before overflowing
		 *                  (subsequent tokens are discarded)
		 * @return this
		 * @throws IllegalArgumentException if {@code maxTokens} is negative or zero
		 */
		@CheckReturnValue
		public ConfigurationUpdater maxTokens(long maxTokens)
		{
			requireThat(maxTokens, "maxTokens").isPositive();
			this.maxTokens = maxTokens;
			return this;
		}

		/**
		 * Sets the minimum number of tokens to add to the limit.
		 *
		 * @param minimumToRefill the minimum number of tokens to add to the limit
		 * @return this
		 * @throws IllegalArgumentException if {@code minimumToRefill} is negative
		 */
		@CheckReturnValue
		public ConfigurationUpdater minimumToRefill(long minimumToRefill)
		{
			requireThat(minimumToRefill, "minimumToRefill").isPositive();
			this.minimumToRefill = minimumToRefill;
			return this;
		}

		/**
		 * Sets the last time that this Limit was refilled.
		 *
		 * @param lastRefilledAt the last time that this Limit was refilled
		 * @return this
		 * @throws NullPointerException if {@code lastRefilledAt} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater lastRefilledAt(Instant lastRefilledAt)
		{
			requireThat(lastRefilledAt, "lastRefilledAt").isNotNull();
			this.lastRefilledAt = lastRefilledAt;
			return this;
		}

		/**
		 * Sets the number of available tokens.
		 *
		 * @param tokensAvailable the number of available tokens
		 * @return this
		 * @throws IllegalArgumentException if {@code tokensAvailable} is negative
		 */
		@CheckReturnValue
		public ConfigurationUpdater tokensAvailable(long tokensAvailable)
		{
			requireThat(tokensAvailable, "tokensAvailable").isNotNegative();
			this.tokensAvailable = tokensAvailable;
			return this;
		}

		/**
		 * Updates this Limit's configuration.
		 */
		public void apply()
		{
			Limit.this.tokensPerPeriod = tokensPerPeriod;
			Limit.this.period = period;
			Limit.this.initialTokens = initialTokens;
			Limit.this.maxTokens = maxTokens;
			Limit.this.minimumToRefill = minimumToRefill;
			Limit.this.lastRefilledAt = lastRefilledAt;
			Limit.this.tokensAvailable = tokensAvailable;
		}
	}
}