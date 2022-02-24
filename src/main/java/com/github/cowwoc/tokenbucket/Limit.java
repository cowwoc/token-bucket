package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
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
	Bucket bucket;
	ReadWriteLockAsResource lock;

	long tokensPerPeriod;
	Duration period;
	long initialTokens;
	long maximumTokens;
	long minimumRefill;
	private Object userData;
	Instant startOfCurrentPeriod;
	long tokensAddedInCurrentPeriod;
	Instant lastRefilledAt;
	long availableTokens;

	/**
	 * Adds a limit that the bucket must respect.
	 * <p>
	 * By default,
	 * <ul>
	 * <li>{@code tokensPerPeriod} is 1.</li>
	 * <li>{@code period} is 1 second.</li>
	 * <li>{@code initialTokens} is 0.</li>
	 * <li>{@code maximumTokens} is {@code Long.MAX_VALUE}.</li>
	 * <li>{@code minimumRefill} is 1.</li>
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
	 * @param maximumTokens   the maximum amount of tokens that the bucket may hold before overflowing
	 *                        (subsequent tokens are discarded)
	 * @param minimumRefill   the minimum number of tokens by which the limit may be refilled
	 * @param userData        the data associated with this limit
	 * @throws NullPointerException     if {@code period} is null
	 * @throws IllegalArgumentException if {@code initialTokens > maximumTokens} or
	 *                                  {@code tokensPerPeriod > maximumTokens}
	 */
	private Limit(long tokensPerPeriod, Duration period, long initialTokens, long maximumTokens,
	              long minimumRefill, Object userData)
	{
		// Assume that all other preconditions are enforced by Builder
		requireThat(maximumTokens, "maximumTokens").
			isGreaterThanOrEqualTo(tokensPerPeriod, "tokensPerPeriod").
			isGreaterThanOrEqualTo(initialTokens, "initialTokens");
		this.tokensPerPeriod = tokensPerPeriod;
		this.period = period;
		this.initialTokens = initialTokens;
		this.maximumTokens = maximumTokens;
		this.minimumRefill = minimumRefill;
		this.userData = userData;

		this.availableTokens = initialTokens;
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
	 * Returns the initial amount of tokens that the bucket starts with.
	 *
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
	 * Returns the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens
	 * are discarded).
	 *
	 * @return the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens
	 * are discarded)
	 */
	public long getMaximumTokens()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return maximumTokens;
		}
	}

	/**
	 * Returns the minimum number of tokens that may be refilled at a time. When completing a
	 * {@link #getPeriod() period} a smaller number of tokens may be refilled in order to avoid surpassing
	 * {@link #getTokensPerPeriod() tokensPerPeriod}.
	 *
	 * @return the minimum number of tokens by which the limit may be refilled
	 */
	public long getMinimumRefill()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return minimumRefill;
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
	long getAvailableTokens()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return availableTokens;
		}
	}

	/**
	 * Returns the maximum number of tokens that can be added before surpassing this Limit's capacity.
	 *
	 * @return the maximum number of tokens that can be added before surpassing this Limit's capacity
	 */
	long getSpaceLeft()
	{
		return maximumTokens - availableTokens;
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
			// In line with the illusion that we are filling the bucket in real-time, "minimumRefill" only
			// applies to the latest period.
			if (tokensToAddInPeriod >= minimumRefill)
			{
				tokensToAdd += tokensToAddInPeriod;
				tokensAddedInCurrentPeriod += tokensToAddInPeriod;
			}
		}
		if (tokensToAdd == 0)
			return 0;

		// Overflow the bucket if necessary
		long tokensBefore = availableTokens;
		availableTokens = Math.min(maximumTokens, saturatedAdd(availableTokens, tokensToAdd));
		return availableTokens - tokensBefore;
	}

	/**
	 * Adds tokens to the limit. The refill rate is not impacted. Limits will drop any tokens added past
	 * {@code maximumTokens}.
	 *
	 * @param tokens the number of tokens to add to the bucket
	 * @return true if the number of available tokens is positive
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero
	 */
	@CheckReturnValue
	boolean addTokens(long tokens)
	{
		assertThat(tokens, "tokens").isPositive();
		availableTokens = saturatedAdd(availableTokens, tokens);
		assertThat(availableTokens, "availableTokens").isLessThanOrEqualTo(maximumTokens, "maximumTokens");
		return availableTokens > 0;
	}

	/**
	 * Consumes tokens.
	 *
	 * @param tokens the number of tokens
	 * @return the number of tokens available after consumption
	 * @throws IllegalArgumentException if {@code tokens > availableTokens}
	 */
	long consume(long tokens)
	{
		assertThat(tokens, "tokens").isLessThanOrEqualTo(availableTokens, "availableTokens");
		availableTokens -= tokens;
		return availableTokens;
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
	 * Simulates consumption with respect to a single limit.
	 *
	 * @param minimumTokens the minimum number of tokens that were requested
	 * @param maximumTokens the maximum number of tokens that were requested
	 * @param requestedAt   the time at which the tokens were requested
	 * @return the simulated consumption
	 * @throws IllegalArgumentException if the limit has a {@code maximumTokens} that is less than
	 *                                  {@code minimumTokens}
	 */
	ConsumptionSimulation simulateConsumption(long minimumTokens, long maximumTokens, Instant requestedAt)
	{
		assertThat(minimumTokens, "minimumTokens").isPositive();
		assertThat(maximumTokens, "maximumTokens").isPositive().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		Instant availableAt;
		long tokensConsumed;
		if (availableTokens < minimumTokens)
		{
			long tokensNeeded = minimumTokens - availableTokens;
			double minimumRefillsNeeded = (double) tokensNeeded / minimumTokens;
			double periodPerRefill = (double) minimumRefill / tokensPerPeriod;
			long secondsToAdd = (long) Math.ceil(period.toSeconds() * minimumRefillsNeeded * periodPerRefill);
			availableAt = lastRefilledAt.plusSeconds(secondsToAdd);
			tokensConsumed = 0;
		}
		else
		{
			availableAt = requestedAt;
			tokensConsumed = Math.min(maximumTokens, availableTokens);
		}
		return new ConsumptionSimulation(tokensConsumed, requestedAt, availableAt);
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
		return Objects.hash(tokensPerPeriod, period, initialTokens, maximumTokens, minimumRefill);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof Limit other)) return false;
		return tokensPerPeriod == other.tokensPerPeriod && initialTokens == other.initialTokens &&
			maximumTokens == other.maximumTokens && period.equals(other.period) &&
			minimumRefill == other.minimumRefill;
	}

	@Override
	public String toString()
	{
		return "availableTokens: " + availableTokens + ", lastRefilledAt: " + lastRefilledAt +
			", tokensPerPeriod: " + tokensPerPeriod + ", period: " + period + ", initialTokens: " +
			initialTokens + ", maximumTokens: " + maximumTokens + ", minimumRefill: " + minimumRefill +
			", userData: " + userData;
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
		private long maximumTokens = Long.MAX_VALUE;
		private long minimumRefill = 1;
		private Object userData;

		/**
		 * Adds a limit that the bucket must respect.
		 * <p>
		 * By default,
		 * <ul>
		 * <li>{@code tokensPerPeriod} is 1.</li>
		 * <li>{@code period} is 1 second.</li>
		 * <li>{@code initialTokens} is 0.</li>
		 * <li>{@code maximumTokens} is {@code Long.MAX_VALUE}.</li>
		 * <li>{@code minimumRefill} is 1.</li>
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
		 * Returns the amount of tokens to add to the bucket every {@code period}.
		 *
		 * @return the amount of tokens to add to the bucket every {@code period}
		 */
		@CheckReturnValue
		public long tokensPerPeriod()
		{
			return tokensPerPeriod;
		}

		/**
		 * Sets the amount of tokens to add to the bucket every {@code period}.
		 *
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
		 * Indicates how often {@code tokensPerPeriod} should be added to the bucket.
		 *
		 * @return how often {@code tokensPerPeriod} should be added to the bucket
		 */
		@CheckReturnValue
		public Duration period()
		{
			return period;
		}

		/**
		 * Indicates how often {@code tokensPerPeriod} should be added to the bucket.
		 *
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
		 * Returns the initial amount of tokens in the bucket. The value may be negative, in which case the
		 * bucket must accumulate a positive number of tokens before they may be consumed.
		 *
		 * @return the initial amount of tokens in the bucket
		 */
		@CheckReturnValue
		public long initialTokens()
		{
			return initialTokens;
		}

		/**
		 * Sets the initial amount of tokens in the bucket. The value may be negative, in which case the
		 * bucket must accumulate a positive number of tokens before they may be consumed.
		 *
		 * @param initialTokens the initial amount of tokens in the bucket
		 * @return this
		 */
		@CheckReturnValue
		public Builder initialTokens(long initialTokens)
		{
			this.initialTokens = initialTokens;
			return this;
		}

		/**
		 * Returns the maximum amount of tokens that the bucket may hold before overflowing.
		 *
		 * @return the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens
		 * are discarded)
		 */
		@CheckReturnValue
		public long maximumTokens()
		{
			return maximumTokens;
		}

		/**
		 * Sets the maximum amount of tokens that the bucket may hold before overflowing.
		 *
		 * @param maximumTokens the maximum amount of tokens that the bucket may hold before overflowing
		 *                      (subsequent tokens are discarded)
		 * @return this
		 * @throws IllegalArgumentException if {@code maximumTokens} is negative or zero
		 */
		@CheckReturnValue
		public Builder maximumTokens(long maximumTokens)
		{
			requireThat(maximumTokens, "maximumTokens").isPositive();
			this.maximumTokens = maximumTokens;
			return this;
		}

		/**
		 * Returns the minimum number of tokens by which the limit may be refilled.
		 *
		 * @return the minimum number of tokens by which the limit may be refilled
		 */
		@CheckReturnValue
		public long minimumRefill()
		{
			return minimumRefill;
		}

		/**
		 * Sets the minimum number of tokens by which the limit may be refilled.
		 *
		 * @param minimumRefill the minimum number of tokens by which the limit may be refilled
		 * @return this
		 * @throws IllegalArgumentException if {@code minimumRefill} is negative
		 */
		@CheckReturnValue
		public Builder minimumRefill(long minimumRefill)
		{
			requireThat(minimumRefill, "minimumRefill").isPositive();
			this.minimumRefill = minimumRefill;
			return this;
		}

		/**
		 * Returns user data associated with this limit.
		 *
		 * @return the data associated with this limit
		 */
		@CheckReturnValue
		public Object userData()
		{
			return userData;
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
			Limit limit = new Limit(tokensPerPeriod, period, initialTokens, maximumTokens, minimumRefill, userData);
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
		private long tokensPerPeriod;
		private Duration period;
		private long maximumTokens;
		private long minimumRefill;
		private Object userData;
		private Instant lastRefilledAt;
		private long availableTokens;
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
				this.maximumTokens = Limit.this.maximumTokens;
				this.minimumRefill = Limit.this.minimumRefill;
				this.lastRefilledAt = Limit.this.lastRefilledAt;
				this.availableTokens = Limit.this.availableTokens;
				this.startOfCurrentPeriod = Limit.this.startOfCurrentPeriod;
				this.tokensAddedInCurrentPeriod = Limit.this.tokensAddedInCurrentPeriod;
			}
		}

		/**
		 * Returns the amount of tokens to add to the bucket every {@code period}.
		 *
		 * @return the amount of tokens to add to the bucket every {@code period}
		 */
		@CheckReturnValue
		public long tokensPerPeriod()
		{
			return tokensPerPeriod;
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
		 * @return how often {@code tokensPerPeriod} should be added to the bucket
		 */
		@CheckReturnValue
		public Duration period()
		{
			return period;
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
		 * Returns the maximum amount of tokens that the bucket may hold before overflowing.
		 *
		 * @return the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens
		 * are discarded)
		 */
		@CheckReturnValue
		public long maximumTokens()
		{
			return maximumTokens;
		}

		/**
		 * Sets the maximum amount of tokens that the bucket may hold before overflowing.
		 *
		 * @param maximumTokens the maximum amount of tokens that the bucket may hold before overflowing
		 *                      (subsequent tokens are discarded)
		 * @return this
		 * @throws IllegalArgumentException if {@code maximumTokens} is negative or zero
		 */
		@CheckReturnValue
		public ConfigurationUpdater maximumTokens(long maximumTokens)
		{
			requireThat(maximumTokens, "maximumTokens").isPositive();
			if (maximumTokens == this.maximumTokens)
				return this;
			changed = true;
			this.maximumTokens = maximumTokens;
			return this;
		}

		/**
		 * Returns the minimum number of tokens by which the limit may be refilled.
		 *
		 * @return the minimum number of tokens by which the limit may be refilled
		 */
		@CheckReturnValue
		public long minimumRefill()
		{
			return minimumRefill;
		}

		/**
		 * Sets the minimum number of tokens by which the limit may be refilled.
		 *
		 * @param minimumRefill the minimum number of tokens by which the limit may be refilled
		 * @return this
		 * @throws IllegalArgumentException if {@code minimumRefill} is negative
		 */
		@CheckReturnValue
		public ConfigurationUpdater minimumRefill(long minimumRefill)
		{
			requireThat(minimumRefill, "minimumRefill").isPositive();
			if (minimumRefill == this.minimumRefill)
				return this;
			changed = true;
			this.minimumRefill = minimumRefill;
			return this;
		}

		/**
		 * Returns the last time that this Limit was refilled.
		 *
		 * @return the last time that this Limit was refilled
		 */
		@CheckReturnValue
		public Instant lastRefilledAt()
		{
			return lastRefilledAt;
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
		 * Returns the number of available tokens. The value may be negative, in which case the bucket must
		 * accumulate a positive number of tokens before they may be consumed.
		 *
		 * @return the number of available tokens
		 */
		@CheckReturnValue
		public long availableTokens()
		{
			return availableTokens;
		}

		/**
		 * Sets the number of available tokens. The value may be negative, in which case the bucket must
		 * accumulate a positive number of tokens before they may be consumed.
		 *
		 * @param availableTokens the number of available tokens
		 * @return this
		 */
		@CheckReturnValue
		public ConfigurationUpdater availableTokens(long availableTokens)
		{
			if (availableTokens == this.availableTokens)
				return this;
			changed = true;
			this.availableTokens = availableTokens;
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
		 * @return the time that the current period has started
		 */
		@CheckReturnValue
		public Instant startOfCurrentPeriod()
		{
			return startOfCurrentPeriod;
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
		 * Returns the number of tokens that were added since {@code startOfCurrentPeriod}.
		 * <p>
		 * Possible values include:
		 * <ul>
		 *   <li>{@code unchanged} to start {@code tokensPerPeriod} relative to the old configuration's period.
		 *   </li>
		 *   <li>{@code 0} to start a new period immediately.</li>
		 * </ul>
		 *
		 * @return the number of tokens that were added since {@code startOfCurrentPeriod}
		 */
		@CheckReturnValue
		public long tokensAddedInCurrentPeriod()
		{
			return tokensAddedInCurrentPeriod;
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
		 * @throws IllegalArgumentException if {@code tokensAddedInCurrentPeriod} is negative
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
		 * Returns the data associated with this limit.
		 *
		 * @return the data associated with this limit
		 */
		@CheckReturnValue
		public Object userData()
		{
			return userData;
		}

		/**
		 * Sets the data associated with this limit.
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
				bucket.updateChild(Limit.this, () ->
				{
					Limit.this.tokensPerPeriod = tokensPerPeriod;
					Limit.this.period = period;
					Limit.this.maximumTokens = maximumTokens;
					Limit.this.minimumRefill = minimumRefill;
					Limit.this.lastRefilledAt = lastRefilledAt;
					Limit.this.userData = userData;

					// Overflow the bucket if necessary
					Limit.this.availableTokens = Math.min(maximumTokens, availableTokens);

					Limit.this.startOfCurrentPeriod = startOfCurrentPeriod;
					Limit.this.tokensAddedInCurrentPeriod = tokensAddedInCurrentPeriod;
				});
			}
		}
	}

	/**
	 * The result of a simulated token consumption.
	 */
	@SuppressWarnings("ClassCanBeRecord")
	static final class ConsumptionSimulation
	{
		private final long tokensConsumed;
		private final Instant requestedAt;
		private final Instant availableAt;

		/**
		 * Creates the result of a simulated token consumption.
		 *
		 * @param tokensConsumed the number of tokens that would be consumed
		 * @param requestedAt    the time at which the tokens were requested
		 * @param availableAt    the time at which the tokens will become available
		 * @throws NullPointerException     if any of the arguments are null
		 * @throws IllegalArgumentException if {@code tokensConsumed} is negative
		 */
		ConsumptionSimulation(long tokensConsumed, Instant requestedAt, Instant availableAt)
		{
			assertThat(tokensConsumed, "tokensConsumed").isNotNegative();
			assertThat(requestedAt, "requestedAt").isNotNull();
			assertThat(availableAt, "availableAt").isNotNull();
			this.tokensConsumed = tokensConsumed;
			this.requestedAt = requestedAt;
			this.availableAt = availableAt;
		}

		/**
		 * Returns true if the bucket was able to consume any tokens.
		 *
		 * @return true if the bucket was able to consume any tokens
		 */
		public boolean isSuccessful()
		{
			return tokensConsumed >= 0;
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
		 * Indicates when the requested number of tokens will become available.
		 *
		 * @return the time when the requested number of tokens will become available
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

		@Override
		public String toString()
		{
			return "successful: " + isSuccessful() + ", tokensConsumed: " + tokensConsumed + ", requestedAt: " +
				requestedAt + ", availableAt: " + availableAt;
		}
	}
}