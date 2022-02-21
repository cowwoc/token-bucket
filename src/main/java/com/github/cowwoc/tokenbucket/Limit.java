package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.Parent;
import com.github.cowwoc.tokenbucket.internal.ReadWriteLockAsResource;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A rate limit.
 */
public final class Limit
{
	// Set by Bucket.Builder.build()
	Parent parent;
	ReadWriteLockAsResource lock;

	long tokensPerPeriod;
	Duration period;
	long initialTokens;
	long maxTokens;
	long minimumToRefill;
	private Object userData;
	Instant startOfCurrentPeriod;
	long tokensAddedInCurrentPeriod;
	Instant lastRefilledAt;
	long tokensAvailable;

	/**
	 * Adds a limit that the bucket must respect.
	 * <p>
	 * By default,
	 * <ul>
	 * <li>{@code tokensPerPeriod} is 1.</li>
	 * <li>{@code period} is 1 second.</li>
	 * <li>{@code initialTokens} is 0.</li>
	 * <li>{@code maxTokens} is {@code Long.MAX_VALUE}.</li>
	 * <li>{@code minimumToRefill} is 1.</li>
	 * <li>{@code userData} is {@code null}.</li>
	 * </ul>
	 *
	 * @return a Limit builder
	 */
	public static Builder builder()
	{
		return new Builder(limit ->
		{
		});
	}

	/**
	 * Creates a new limit.
	 *
	 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
	 * @param period          indicates how often {@code tokensPerPeriod} should be added to the bucket
	 * @param initialTokens   the initial amount of tokens in the bucket
	 * @param maxTokens       the maximum amount of tokens that the bucket may hold before overflowing
	 *                        (subsequent tokens are discarded)
	 * @param minimumToRefill the minimum number of tokens by which the limit may be refilled
	 * @param userData        the data associated with this limit
	 * @throws NullPointerException     if {@code period} is null
	 * @throws IllegalArgumentException if {@code initialTokens > maxTokens} or
	 *                                  {@code tokensPerPeriod > maxTokens}
	 */
	private Limit(long tokensPerPeriod, Duration period, long initialTokens, long maxTokens,
	              long minimumToRefill, Object userData)
	{
		// Assume that all other preconditions are enforced by Builder
		requireThat(maxTokens, "maxTokens").
			isGreaterThanOrEqualTo(tokensPerPeriod, "tokensPerPeriod").
			isGreaterThanOrEqualTo(initialTokens, "initialTokens");
		this.tokensPerPeriod = tokensPerPeriod;
		this.period = period;
		this.initialTokens = initialTokens;
		this.maxTokens = maxTokens;
		this.minimumToRefill = minimumToRefill;
		this.userData = userData;

		this.tokensAvailable = initialTokens;
		this.lastRefilledAt = Instant.now();
		this.startOfCurrentPeriod = lastRefilledAt;
		this.tokensAddedInCurrentPeriod = 0;
	}

	/**
	 * Returns the number of tokens to add every {@code period}.
	 *
	 * @return the number of tokens to add every {@code period}
	 */
	public long getTokensPerPeriod()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return tokensPerPeriod;
		}
	}

	/**
	 * indicates how often {@code tokensPerPeriod} should be added to the bucket.
	 *
	 * @return indicates how often {@code tokensPerPeriod} should be added to the bucket
	 */
	public Duration getPeriod()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return period;
		}
	}

	/**
	 * @return the initial amount of tokens that the bucket starts with
	 */
	public long getInitialTokens()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return initialTokens;
		}
	}

	/**
	 * @return the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens are discarded)
	 */
	public long getMaxTokens()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return maxTokens;
		}
	}

	/**
	 * Returns the minimum number of tokens that may be refilled at a time. When completing a
	 * {@link #getPeriod() period} a smaller number of tokens may be refilled in order to avoid surpassing
	 * {@link #getTokensPerPeriod() tokensPerPeriod}.
	 *
	 * @return the minimum number of tokens by which the limit may be refilled
	 */
	public long getMinimumToRefill()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return minimumToRefill;
		}
	}

	/**
	 * Returns the data associated with this limit.
	 *
	 * @return the data associated with this limit
	 */
	public Object getUserData()
	{
		return userData;
	}

	/**
	 * Returns the number of tokens that are available, without triggering a refill.
	 *
	 * @return the number of tokens that are available
	 */
	long getTokensAvailable()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return tokensAvailable;
		}
	}

	/**
	 * @return the last time that tokens were refilled
	 */
	public Instant getLastRefilledAt()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return lastRefilledAt;
		}
	}

	/**
	 * Returns the maximum number of tokens that can be added before surpassing this Limit's capacity.
	 *
	 * @return the maximum number of tokens that can be added before surpassing this Limit's capacity
	 */
	long getSpaceLeft()
	{
		return maxTokens - tokensAvailable;
	}

	/**
	 * Refills the limit.
	 *
	 * @param requestedAt the time that the tokens were requested at
	 * @return the number of tokens added
	 * @throws NullPointerException if {@code requestedAt} is null
	 */
	long refill(Instant requestedAt)
	{
		Duration timeElapsed = Duration.between(startOfCurrentPeriod, requestedAt);
		if (timeElapsed.isNegative())
			return 0;
		long tokensToAdd = 0;
		long numberOfPeriodsElapsed = timeElapsed.dividedBy(period);
		if (numberOfPeriodsElapsed > 0)
		{
			tokensToAdd += numberOfPeriodsElapsed * tokensPerPeriod - tokensAddedInCurrentPeriod;
			tokensAddedInCurrentPeriod = 0;
			startOfCurrentPeriod = startOfCurrentPeriod.plus(period.multipliedBy(numberOfPeriodsElapsed));
			lastRefilledAt = startOfCurrentPeriod;
		}
		Duration timeElapsedInPeriod = timeElapsed.minus(period.multipliedBy(numberOfPeriodsElapsed));
		if (!timeElapsedInPeriod.isZero())
		{
			// The amount of time that must elapse for a single token to get added
			Duration durationPerToken = period.dividedBy(tokensPerPeriod);
			// Fill tokens smoothly. For example, a refill rate of 60 tokens a minute will add 1 token per second
			// as opposed to 60 tokens all at once at the end of the minute.
			long tokensToAddInPeriod = timeElapsedInPeriod.dividedBy(durationPerToken) -
				tokensAddedInCurrentPeriod;
			// In line with the illusion that we are filling the bucket in real-time, "minimumToRefill" only
			// applies to the latest period.
			if (tokensToAddInPeriod >= minimumToRefill)
			{
				tokensToAdd += tokensToAddInPeriod;
				tokensAddedInCurrentPeriod += tokensToAddInPeriod;
			}
		}
		if (tokensToAdd == 0)
			return 0;

		// Overflow the bucket if necessary
		long tokensBefore = tokensAvailable;
		tokensAvailable = Math.min(maxTokens, saturatedAdd(tokensAvailable, tokensToAdd));
		return tokensAvailable - tokensBefore;
	}

	/**
	 * Adds tokens to the limit. The refill rate is not impacted. Limits will drop any tokens added past
	 * {@code maxTokens}.
	 *
	 * @param tokens the number of tokens to add to the bucket
	 * @throws IllegalArgumentException if {@code tokens} is negative
	 */
	@CheckReturnValue
	void addTokens(long tokens)
	{
		assertThat(tokens, "tokens").isNotNegative();
		tokensAvailable = saturatedAdd(tokensAvailable, tokens);
		assertThat(tokensAvailable, "tokensAvailable").isLessThanOrEqualTo(maxTokens, "maxTokens");
	}

	/**
	 * Consumes tokens.
	 *
	 * @param tokens the number of tokens
	 * @throws IllegalArgumentException if {@code tokens > tokensAvailable}
	 */
	void consume(long tokens)
	{
		assertThat(tokens, "tokens").isLessThanOrEqualTo(tokensAvailable, "tokensAvailable");
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
	 * tokens should avoid accessing the enclosing bucket until the configuration update is complete.
	 *
	 * @return the configuration updater
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
			maxTokens == other.maxTokens && period.equals(other.period) &&
			minimumToRefill == other.minimumToRefill;
	}

	@Override
	public String toString()
	{
		return "tokensAvailable: " + tokensAvailable + ", lastRefilledAt: " + lastRefilledAt +
			", tokensPerPeriod: " + tokensPerPeriod + ", period: " + period + ", initialTokens: " +
			initialTokens + ", maxTokens: " + maxTokens + ", minimumToRefill: " + minimumToRefill +
			"userData: " + userData;
	}

	/**
	 * Builds a Limit.
	 */
	public static final class Builder
	{
		private final Consumer<Limit> consumer;
		private long tokensPerPeriod = 1;
		private Duration period = Duration.ofSeconds(1);
		private long initialTokens;
		private long maxTokens = Long.MAX_VALUE;
		private long minimumToRefill = 1;
		private Object userData;

		/**
		 * Adds a limit that the bucket must respect.
		 * <p>
		 * By default,
		 * <ul>
		 * <li>{@code tokensPerPeriod} is 1.</li>
		 * <li>{@code period} is 1 second.</li>
		 * <li>{@code initialTokens} is 0.</li>
		 * <li>{@code maxTokens} is {@code Long.MAX_VALUE}.</li>
		 * <li>{@code minimumToRefill} is 1.</li>
		 * <li>{@code userData} is {@code null}.</li>
		 * </ul>
		 *
		 * @param consumer consumes the bucket before it is returned
		 * @throws NullPointerException if {@code consumer} is null
		 */
		Builder(Consumer<Limit> consumer)
		{
			assertThat(consumer, "consumer").isNotNull();
			this.consumer = consumer;
		}

		/**
		 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
		 * @return this
		 * @throws IllegalArgumentException if {@code tokensPerPeriod} is negative or zero
		 */
		@CheckReturnValue
		public Builder tokensPerPeriod(long tokensPerPeriod)
		{
			requireThat(tokensPerPeriod, "tokensPerPeriod").isPositive();
			this.tokensPerPeriod = tokensPerPeriod;
			return this;
		}

		/**
		 * @param period indicates how often {@code tokensPerPeriod} should be added to the bucket
		 * @return this
		 * @throws IllegalArgumentException if {@code period} is negative or zero
		 * @throws NullPointerException     if {@code period} is null
		 */
		@CheckReturnValue
		public Builder period(Duration period)
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
		public Builder initialTokens(long initialTokens)
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
		public Builder maxTokens(long maxTokens)
		{
			requireThat(maxTokens, "maxTokens").isPositive();
			this.maxTokens = maxTokens;
			return this;
		}

		/**
		 * Sets the minimum number of tokens by which the limit may be refilled.
		 *
		 * @param minimumToRefill the minimum number of tokens by which the limit may be refilled
		 * @return this
		 * @throws IllegalArgumentException if {@code minimumToRefill} is negative
		 */
		@CheckReturnValue
		public Builder minimumToRefill(long minimumToRefill)
		{
			requireThat(minimumToRefill, "minimumToRefill").isPositive();
			this.minimumToRefill = minimumToRefill;
			return this;
		}

		/**
		 * Sets user data associated with this limit.
		 *
		 * @param userData the data associated with this limit
		 * @return this
		 */
		@CheckReturnValue
		public Builder userData(Object userData)
		{
			this.userData = userData;
			return this;
		}

		/**
		 * Builds a new Limit.
		 *
		 * @return a new Limit
		 */
		public Limit build()
		{
			Limit limit = new Limit(tokensPerPeriod, period, initialTokens, maxTokens, minimumToRefill, userData);
			consumer.accept(limit);
			return limit;
		}
	}

	/**
	 * Updates this Limit's configuration.
	 * <p>
	 * <b>Thread-safety</b>: This class is not thread-safe.
	 */
	public final class ConfigurationUpdater
	{
		private Object userData;
		private long tokensPerPeriod;
		private Duration period;
		private long initialTokens;
		private long maxTokens;
		private long minimumToRefill;
		private Instant lastRefilledAt;
		private long tokensAvailable;
		private Instant startOfCurrentPeriod;
		private long tokensAddedInCurrentPeriod;
		private boolean changed;

		/**
		 * Creates a new configuration updater.
		 */
		private ConfigurationUpdater()
		{
			try (CloseableLock ignored = lock.readLock())
			{
				this.userData = Limit.this.userData;
				this.tokensPerPeriod = Limit.this.tokensPerPeriod;
				this.period = Limit.this.period;
				this.initialTokens = Limit.this.initialTokens;
				this.maxTokens = Limit.this.maxTokens;
				this.minimumToRefill = Limit.this.minimumToRefill;
				this.lastRefilledAt = Limit.this.lastRefilledAt;
				this.tokensAvailable = Limit.this.tokensAvailable;
				this.startOfCurrentPeriod = Limit.this.startOfCurrentPeriod;
				this.tokensAddedInCurrentPeriod = Limit.this.tokensAddedInCurrentPeriod;
			}
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
			if (tokensPerPeriod == this.tokensPerPeriod)
				return this;
			this.changed = true;
			this.tokensPerPeriod = tokensPerPeriod;
			return this;
		}

		/**
		 * Indicates how often {@code tokensPerPeriod} should be added to the bucket.
		 *
		 * @param period indicates how often {@code tokensPerPeriod} should be added to the bucket
		 * @return this
		 * @throws NullPointerException if {@code period} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater period(Duration period)
		{
			requireThat(period, "period").isGreaterThan(Duration.ZERO);
			if (period.equals(this.period))
				return this;
			changed = true;
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
			if (initialTokens == this.initialTokens)
				return this;
			changed = true;
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
			if (maxTokens == this.maxTokens)
				return this;
			changed = true;
			this.maxTokens = maxTokens;
			return this;
		}

		/**
		 * Sets the minimum number of tokens by which the limit may be refilled.
		 *
		 * @param minimumToRefill the minimum number of tokens by which the limit may be refilled
		 * @return this
		 * @throws IllegalArgumentException if {@code minimumToRefill} is negative
		 */
		@CheckReturnValue
		public ConfigurationUpdater minimumToRefill(long minimumToRefill)
		{
			requireThat(minimumToRefill, "minimumToRefill").isPositive();
			if (minimumToRefill == this.minimumToRefill)
				return this;
			changed = true;
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
			if (lastRefilledAt.equals(this.lastRefilledAt))
				return this;
			changed = true;
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
			if (tokensAvailable == this.tokensAvailable)
				return this;
			changed = true;
			this.tokensAvailable = tokensAvailable;
			return this;
		}

		/**
		 * Indicates when the most recent period has started.
		 * <p>
		 * Possible values include:
		 * <ul>
		 *   <li>{@code unchanged} to start {@code tokensPerPeriod} relative to the old configuration's period.</li>
		 *   <li>{@code lastUpdatedAt} to start a new period immediately.</li>
		 * </ul>
		 *
		 * @param startOfCurrentPeriod the time that the current period has started
		 * @return this
		 * @throws NullPointerException if {@code startOfCurrentPeriod} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater startOfCurrentPeriod(Instant startOfCurrentPeriod)
		{
			requireThat(startOfCurrentPeriod, "startOfCurrentPeriod").isNotNull();
			if (startOfCurrentPeriod.equals(this.startOfCurrentPeriod))
				return this;
			changed = true;
			this.startOfCurrentPeriod = startOfCurrentPeriod;
			return this;
		}

		/**
		 * Sets the number of tokens that were added since {@code startOfCurrentPeriod}.
		 * <p>
		 * Possible values include:
		 * <ul>
		 *   <li>{@code unchanged} to start {@code tokensPerPeriod} relative to the old configuration's period.
		 *   </li>
		 *   <li>{@code 0} to start a new period immediately.</li>
		 * </ul>
		 *
		 * @param tokensAddedInCurrentPeriod the number of tokens that were added since
		 *                                   {@code startOfCurrentPeriod}
		 * @return this
		 * @throws IllegalArgumentException if {@code tokensAddedSinceLastPeriod} is negative
		 */
		@CheckReturnValue
		public ConfigurationUpdater tokensAddedInCurrentPeriod(int tokensAddedInCurrentPeriod)
		{
			requireThat(tokensAddedInCurrentPeriod, "tokensAddedInCurrentPeriod").isNotNegative();
			if (tokensAddedInCurrentPeriod == this.tokensAddedInCurrentPeriod)
				return this;
			changed = true;
			this.tokensAddedInCurrentPeriod = tokensAddedInCurrentPeriod;
			return this;
		}

		/**
		 * Sets an object that uniquely identifies this limit.
		 *
		 * @param userData the data associated with this limit
		 * @return this
		 */
		@CheckReturnValue
		public ConfigurationUpdater userData(Object userData)
		{
			if (Objects.equals(userData, this.userData))
				return this;
			changed = true;
			this.userData = userData;
			return this;
		}

		/**
		 * Updates this Limit's configuration.
		 */
		public void apply()
		{
			if (!changed)
				return;
			try (CloseableLock ignored = lock.writeLock())
			{
				parent.updateChild(Limit.this, () ->
				{
					Limit.this.userData = userData;
					Limit.this.tokensPerPeriod = tokensPerPeriod;
					Limit.this.period = period;
					Limit.this.initialTokens = initialTokens;
					Limit.this.maxTokens = maxTokens;
					Limit.this.minimumToRefill = minimumToRefill;
					Limit.this.lastRefilledAt = lastRefilledAt;

					// Overflow the bucket if necessary
					Limit.this.tokensAvailable = Math.min(maxTokens, tokensAvailable);

					Limit.this.startOfCurrentPeriod = startOfCurrentPeriod;
					Limit.this.tokensAddedInCurrentPeriod = tokensAddedInCurrentPeriod;
				});
			}
		}
	}
}