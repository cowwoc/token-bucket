package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.Requirements;
import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.ReentrantStampedLock;
import com.github.cowwoc.tokenbucket.internal.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A rate limit.
 * <p>
 * <b>Thread safety</b>: This class is thread-safe.
 */
public final class Limit
{
	Bucket bucket;

	long tokensPerPeriod;
	Duration period;
	private final long initialTokens;
	long maximumTokens;
	long refillSize;
	private Object userData;
	Instant startOfCurrentPeriod;
	Instant nextRefillAt;
	long tokensAddedInCurrentPeriod;
	long availableTokens;
	long refillsElapsed;
	Duration timePerToken;
	Duration timePerRefill;
	long refillsPerPeriod;
	/**
	 * A lock over this object's state. See the {@link com.github.cowwoc.tokenbucket.internal locking policy}
	 * for more details.
	 */
	final ReentrantStampedLock lock = new ReentrantStampedLock();
	private final Logger log = LoggerFactory.getLogger(Limit.class);

	/**
	 * Creates a new limit.
	 *
	 * @param tokensPerPeriod the number of tokens to add to the bucket every {@code period}
	 * @param period          indicates how often {@code tokensPerPeriod} should be added to the bucket
	 * @param initialTokens   the initial number of tokens in the bucket
	 * @param maximumTokens   the maximum number of tokens that the bucket may hold before overflowing
	 *                        (subsequent tokens are discarded)
	 * @param refillSize      the number of tokens that are refilled at a time
	 * @param userData        the data associated with this limit
	 * @throws NullPointerException     if {@code period} is null
	 * @throws IllegalArgumentException if {@code initialTokens > maximumTokens} or
	 *                                  {@code tokensPerPeriod > maximumTokens}
	 */
	private Limit(long tokensPerPeriod, Duration period, long initialTokens, long maximumTokens,
	              long refillSize, Object userData)
	{
		// Assume that all other preconditions are enforced by Builder
		requireThat(maximumTokens, "maximumTokens").
			isGreaterThanOrEqualTo(tokensPerPeriod, "tokensPerPeriod").
			isGreaterThanOrEqualTo(initialTokens, "initialTokens");
		this.tokensPerPeriod = tokensPerPeriod;
		this.period = period;
		this.initialTokens = initialTokens;
		this.maximumTokens = maximumTokens;
		this.refillSize = refillSize;
		this.userData = userData;
		this.availableTokens = initialTokens;
		this.timePerToken = period.dividedBy(tokensPerPeriod);
		this.timePerRefill = timePerToken.multipliedBy(refillSize);
		this.startOfCurrentPeriod = Instant.now();
		this.tokensAddedInCurrentPeriod = 0;
		this.refillsElapsed = 0;
		this.refillsPerPeriod = (long) Math.ceil((double) tokensPerPeriod / refillSize);
		this.nextRefillAt = getRefillEndTime(1);
	}

	/**
	 * Returns the bucket containing this limit.
	 *
	 * @return the bucket containing this limit
	 */
	public Bucket getBucket()
	{
		return lock.optimisticReadLock(() -> bucket);
	}

	/**
	 * Returns the number of tokens to add every {@code period}.
	 *
	 * @return the number of tokens to add every {@code period}
	 */
	public long getTokensPerPeriod()
	{
		return lock.optimisticReadLock(() -> tokensPerPeriod);
	}

	/**
	 * indicates how often {@code tokensPerPeriod} should be added to the bucket.
	 *
	 * @return indicates how often {@code tokensPerPeriod} should be added to the bucket
	 */
	public Duration getPeriod()
	{
		return lock.optimisticReadLock(() -> period);
	}

	/**
	 * Returns the initial number of tokens that the bucket starts with.
	 *
	 * @return the initial number of tokens that the bucket starts with
	 */
	public long getInitialTokens()
	{
		return lock.optimisticReadLock(() -> initialTokens);
	}

	/**
	 * Returns the maximum number of tokens that the bucket may hold before overflowing (subsequent tokens
	 * are discarded).
	 *
	 * @return the maximum number of tokens that the bucket may hold before overflowing (subsequent tokens
	 * are discarded)
	 */
	public long getMaximumTokens()
	{
		return lock.optimisticReadLock(() -> maximumTokens);
	}

	/**
	 * Returns the number of tokens that are refilled at a time.
	 * <p>
	 * At one end of the spectrum, one may refill tokens one at a time during the {@link #getPeriod() period}.
	 * On the other spectrum, one may refill all tokens at the end of the {@code period}.
	 * <p>
	 * <b>NOTE</b>: When completing a {@code period}, a smaller number of tokens may be refilled in order to
	 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod} due to rounding errors.
	 *
	 * @return the minimum number of tokens by which the limit may be refilled
	 */
	public long getRefillSize()
	{
		return lock.optimisticReadLock(() -> refillSize);
	}

	/**
	 * Returns the data associated with this limit.
	 *
	 * @return the data associated with this limit
	 */
	public Object getUserData()
	{
		return lock.optimisticReadLock(() -> userData);
	}

	/**
	 * @param consumedAt the time that the tokens are being consumed
	 * @return if {@code consumedAt} is after the next refill time
	 */
	boolean readyForRefill(Instant consumedAt)
	{
		return consumedAt.compareTo(nextRefillAt) >= 0;
	}

	/**
	 * Refills the limit.
	 *
	 * @param consumedAt the time that the tokens are being consumed
	 * @throws NullPointerException if {@code consumedAt} is null
	 * @implNote This method acquires its own locks
	 */
	void refill(Instant consumedAt)
	{
		if (!lock.optimisticReadLock(() -> readyForRefill(consumedAt)))
			return;
		try (CloseableLock ignored = lock.writeLock())
		{
			Requirements requirements = new Requirements();
			requirements.assertThat(r ->
			{
				r.withContext("nextRefillAt", nextRefillAt).
					withContext("startOfCurrentPeriod", startOfCurrentPeriod).
					withContext("consumedAt", consumedAt);
			});
			Duration timeElapsedSinceStartOfPeriod = Duration.between(startOfCurrentPeriod, consumedAt);
			requirements.assertThat(r ->
			{
				r.requireThat(timeElapsedSinceStartOfPeriod, "timeElapsedSinceStartOfPeriod").
					isGreaterThanOrEqualTo(Duration.ZERO);
				r.requireThat(tokensAddedInCurrentPeriod, "tokensAddedInCurrentPeriod").
					isLessThanOrEqualTo(tokensPerPeriod, "tokensPerPeriod");
			});
			long tokensToAdd = 0;
			long numberOfPeriodsElapsed = timeElapsedSinceStartOfPeriod.dividedBy(period);
			if (numberOfPeriodsElapsed > 0)
			{
				requirements.assertThat(r ->
				{
					r.withContext("numberOfPeriodsElapsed", numberOfPeriodsElapsed).
						withContext("tokensPerPeriod", tokensPerPeriod).
						withContext("tokensAddedInCurrentPeriod", tokensAddedInCurrentPeriod);
				});
				tokensToAdd += numberOfPeriodsElapsed * tokensPerPeriod - tokensAddedInCurrentPeriod;
				long finalTokensToAdd = tokensToAdd;
				requirements.assertThat(r ->
				{
					r.requireThat(finalTokensToAdd, "tokensToAdd").isNotNegative();
					r.withContext("tokensToAdd", finalTokensToAdd);
				});
				tokensAddedInCurrentPeriod = 0;
				startOfCurrentPeriod = startOfCurrentPeriod.plus(period.multipliedBy(numberOfPeriodsElapsed));
			}
			Duration timeElapsedInPeriod = timeElapsedSinceStartOfPeriod.
				minus(period.multipliedBy(numberOfPeriodsElapsed));
			refillsElapsed = timeElapsedInPeriod.dividedBy(timePerRefill);
			requirements.assertThat(r -> r.requireThat(timeElapsedInPeriod, "timeElapsedInPeriod").
				isBetweenClosed(timePerRefill.multipliedBy(refillsElapsed),
					timePerRefill.multipliedBy(refillsElapsed + 1)));
			if (!timeElapsedInPeriod.isZero())
			{
				// If the refillSize was updated mid-period, it is possible for tokensAddedInCurrentPeriod to be
				// greater than the new refillSize * refillsElapsed. We guard against this using Math.max(0) and
				// treat all future refills as if the new refillSize applied from the beginning of the period.
				long tokensToAddInPeriod = Math.max(0, refillSize * refillsElapsed - tokensAddedInCurrentPeriod);
				requirements.assertThat(r ->
				{
					r.withContext("timePerToken", timePerToken).
						withContext("period", period).
						withContext("tokensPerPeriod", tokensPerPeriod).
						withContext("timePerRefill", timePerRefill).
						withContext("refillSize", refillSize).
						withContext("refillsElapsed", refillsElapsed).
						withContext("timeElapsedInPeriod", timeElapsedInPeriod).
						withContext("tokensAddedInCurrentPeriod", tokensAddedInCurrentPeriod);
					r.requireThat(tokensToAddInPeriod, "tokensToAddInPeriod").isNotNegative();
					r.withContext("tokensToAddInPeriod", tokensToAddInPeriod);
				});
				tokensToAdd += tokensToAddInPeriod;
				tokensAddedInCurrentPeriod += tokensToAddInPeriod;
			}
			long finalTokensToAdd = tokensToAdd;
			requirements.assertThat(r -> r.requireThat(finalTokensToAdd, "tokensToAdd").isNotNegative());

			nextRefillAt = getRefillEndTime(refillsElapsed + 1);
			availableTokens = saturatedAdd(availableTokens, tokensToAdd);
			overflowBucket();
		}
	}

	/**
	 * @param refills a number of refills relative to the beginning of the current period
	 * @return the time at which the last refill will complete
	 */
	Instant getRefillEndTime(long refills)
	{
		Instant result = this.startOfCurrentPeriod;
		long numberOfPeriodsElapsed = refills / refillsPerPeriod;
		if (numberOfPeriodsElapsed > 0)
		{
			refills -= numberOfPeriodsElapsed * refillsPerPeriod;
			result = result.plus(period.multipliedBy(numberOfPeriodsElapsed));
		}

		Duration timeElapsedInPeriod = timePerRefill.multipliedBy(refills);
		return result.plus(timeElapsedInPeriod);
	}

	/**
	 * Overflows the bucket, if necessary.
	 */
	private void overflowBucket()
	{
		availableTokens = Math.min(maximumTokens, availableTokens);
	}

	/**
	 * Consumes tokens.
	 *
	 * @param tokens the number of tokens
	 * @return the number of tokens left after consumption
	 * @throws IllegalArgumentException if {@code tokens > availableTokens}
	 */
	long consume(long tokens)
	{
		assertThat(r -> r.requireThat(tokens, "tokens").
			isLessThanOrEqualTo(availableTokens, "availableTokens"));
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
	SimulatedConsumption simulateConsumption(long minimumTokens, long maximumTokens, Instant requestedAt)
	{
		assertThat(r ->
		{
			r.requireThat(minimumTokens, "minimumTokens").isPositive();
			r.requireThat(maximumTokens, "maximumTokens").isPositive().
				isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		});
		Instant availableAt;
		long tokensConsumed;
		if (availableTokens < minimumTokens)
		{
			long tokensNeeded = minimumTokens - availableTokens;
			long refillsNeeded = (long) Math.ceil((double) tokensNeeded / refillSize);
			if (refillsNeeded == 1)
				availableAt = nextRefillAt;
			else
				availableAt = getRefillEndTime(refillsElapsed + refillsNeeded);
			tokensConsumed = 0;
		}
		else
		{
			availableAt = requestedAt;
			tokensConsumed = Math.min(maximumTokens, availableTokens);
			assertThat(r -> r.requireThat(tokensConsumed, "tokensConsumed").isPositive());
		}
		return new SimulatedConsumption(tokensConsumed, requestedAt, availableAt);
	}

	/**
	 * Updates this Limit's configuration.
	 * <p>
	 * The {@code Limit} will be locked until {@link ConfigurationUpdater#close()} is invoked, at which point a
	 * new period will be started.
	 *
	 * @return the configuration updater
	 */
	@CheckReturnValue
	public ConfigurationUpdater updateConfiguration()
	{
		return new ConfigurationUpdater();
	}

	@Override
	public int hashCode()
	{
		return lock.optimisticReadLock(() ->
			Objects.hash(tokensPerPeriod, period, initialTokens, maximumTokens, refillSize));
	}

	@Override
	public boolean equals(Object o)
	{
		return lock.optimisticReadLock(() ->
		{
			if (!(o instanceof Limit other))
				return false;
			return tokensPerPeriod == other.tokensPerPeriod && initialTokens == other.initialTokens &&
				maximumTokens == other.maximumTokens && period.equals(other.period) &&
				refillSize == other.refillSize;
		});
	}

	@Override
	public String toString()
	{
		return lock.optimisticReadLock(() ->
		{
			ToStringBuilder builder = new ToStringBuilder(Limit.class).
				add("tokensPerPeriod", tokensPerPeriod).
				add("period", period).
				add("maximumTokens", maximumTokens).
				add("refillSize", refillSize).
				add("userData", userData);
			if (log.isDebugEnabled())
				builder.
					add("initialTokens", initialTokens).
					add("startOfCurrentPeriod", startOfCurrentPeriod).
					add("nextRefillAt", nextRefillAt).
					add("tokensAddedInCurrentPeriod", tokensAddedInCurrentPeriod).
					add("availableTokens", availableTokens).
					add("refillsElapsed", refillsElapsed).
					add("timePerToken", timePerToken).
					add("timePerRefill", timePerRefill).
					add("refillsPerPeriod", refillsPerPeriod);
			return builder.toString();
		});
	}

	/**
	 * The result of a simulated token consumption.
	 */
	static final class SimulatedConsumption
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
		 * @throws IllegalArgumentException if {@code tokensConsumed} is negative.
		 *                                  If {@code requestedAt > availableAt}.
		 */
		SimulatedConsumption(long tokensConsumed, Instant requestedAt, Instant availableAt)
		{
			assertThat(r ->
			{
				r.requireThat(tokensConsumed, "tokensConsumed").isNotNegative();
				r.requireThat(requestedAt, "requestedAt").isNotNull();
				r.requireThat(availableAt, "availableAt").isGreaterThanOrEqualTo(requestedAt, "requestedAt");
			});
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
		 * @return the time at which the tokens were requested
		 */
		public Instant getRequestedAt()
		{
			return requestedAt;
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

		@Override
		public String toString()
		{
			return new ToStringBuilder().
				add("successful", isSuccessful()).
				add("tokensConsumed", tokensConsumed).
				add("requestedAt", requestedAt).
				add("availableAt", availableAt).
				toString();
		}
	}

	/**
	 * Builds a Limit.
	 */
	public static final class Builder
	{
		private long tokensPerPeriod = 1;
		private Duration period = Duration.ofSeconds(1);
		private long initialTokens;
		private long maximumTokens = Long.MAX_VALUE;
		private long refillSize = 1;
		private Object userData;

		/**
		 * Prevent construction.
		 */
		Builder()
		{
		}

		/**
		 * Returns the number of tokens to add to the bucket every {@code period}. The default is {@code 1}.
		 *
		 * @return the number of tokens to add to the bucket every {@code period}
		 */
		@CheckReturnValue
		public long tokensPerPeriod()
		{
			return tokensPerPeriod;
		}

		/**
		 * Sets the number of tokens to add to the bucket every {@code period}.
		 *
		 * @param tokensPerPeriod the number of tokens to add to the bucket every {@code period}
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
		 * Indicates how often {@code tokensPerPeriod} should be added to the bucket. The default is
		 * {@code 1 second}.
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
		 * Returns the initial number of tokens in the bucket. The value may be negative, in which case the
		 * bucket must accumulate a positive number of tokens before they may be consumed. The default is
		 * {@code 0}.
		 *
		 * @return the number amount of tokens in the bucket
		 */
		@CheckReturnValue
		public long initialTokens()
		{
			return initialTokens;
		}

		/**
		 * Sets the initial number of tokens in the bucket. The value may be negative, in which case the
		 * bucket must accumulate a positive number of tokens before they may be consumed.
		 *
		 * @param initialTokens the initial number of tokens in the bucket
		 * @return this
		 */
		@CheckReturnValue
		public Builder initialTokens(long initialTokens)
		{
			this.initialTokens = initialTokens;
			return this;
		}

		/**
		 * Returns the maximum number of tokens that the bucket may hold before overflowing. The default is
		 * {@code Long.MAX_VALUE}.
		 *
		 * @return the maximum number of tokens that the bucket may hold before overflowing (subsequent tokens
		 * are discarded)
		 */
		@CheckReturnValue
		public long maximumTokens()
		{
			return maximumTokens;
		}

		/**
		 * Sets the maximum number of tokens that the bucket may hold before overflowing.
		 *
		 * @param maximumTokens the maximum number of tokens that the bucket may hold before overflowing
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
		 * Returns the number of tokens that are refilled at a time.
		 * <p>
		 * At one end of the spectrum, one may refill tokens one at a time during the {@link #getPeriod() period}.
		 * On the other spectrum, one may refill {@code tokensPerPeriod} tokens once per {@code period}.
		 * <p>
		 * <b>NOTE</b>: When completing a {@code period} a smaller number of tokens may be refilled in order to
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod} due to rounding errors.
		 * <p>
		 * The default is {@code 1}.
		 *
		 * @return the minimum number of tokens by which the limit may be refilled
		 */
		@CheckReturnValue
		public long refillSize()
		{
			return refillSize;
		}

		/**
		 * Sets the number of tokens that are refilled at a time.
		 * <p>
		 * At one end of the spectrum, one may refill tokens one at a time during the {@link #getPeriod() period}.
		 * On the other spectrum, one may refill {@code tokensPerPeriod} tokens once per {@code period}.
		 * <p>
		 * <b>NOTE</b>: When completing a {@code period} a smaller number of tokens may be refilled in order to
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod} due to rounding errors.
		 *
		 * @param refillSize the minimum number of tokens by which the limit may be refilled
		 * @return this
		 * @throws IllegalArgumentException if {@code refillSize} is negative or zero
		 */
		@CheckReturnValue
		public Builder refillSize(long refillSize)
		{
			requireThat(refillSize, "refillSize").isPositive();
			this.refillSize = refillSize;
			return this;
		}

		/**
		 * Returns user data associated with this limit. The default is {@code null}.
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
			return new Limit(tokensPerPeriod, period, initialTokens, maximumTokens, refillSize, userData);
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder(Builder.class).
				add("tokensPerPeriod", tokensPerPeriod).
				add("period", period).
				add("refillSize", refillSize).
				add("maximumTokens", maximumTokens).
				add("userData", userData).
				toString();
		}
	}

	/**
	 * Updates this Limit's configuration.
	 * <p>
	 * <b>Thread-safety</b>: This class is not thread-safe.
	 */
	public final class ConfigurationUpdater implements AutoCloseable
	{
		private final CloseableLock writeLock = lock.writeLock();
		private boolean closed;
		private long tokensPerPeriod;
		private Duration period;
		private long availableTokens;
		private long refillSize;
		private long maximumTokens;
		private Object userData;
		private boolean changed;

		/**
		 * Prevent construction.
		 */
		private ConfigurationUpdater()
		{
			this.tokensPerPeriod = Limit.this.tokensPerPeriod;
			this.period = Limit.this.period;
			this.availableTokens = Limit.this.availableTokens;
			this.refillSize = Limit.this.refillSize;
			this.maximumTokens = Limit.this.maximumTokens;
			this.userData = Limit.this.userData;
		}

		/**
		 * Returns the number of tokens to add to the bucket every {@code period}.
		 *
		 * @return the number of tokens to add to the bucket every {@code period}
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public long tokensPerPeriod()
		{
			ensureOpen();
			return tokensPerPeriod;
		}

		/**
		 * Sets the number of tokens to add to the bucket every {@code period}.
		 *
		 * @param tokensPerPeriod the number of tokens to add to the bucket every {@code period}
		 * @return this
		 * @throws IllegalArgumentException if {@code tokensPerPeriod} is negative or zero
		 * @throws IllegalStateException    if the updater is closed
		 */
		public ConfigurationUpdater tokensPerPeriod(long tokensPerPeriod)
		{
			requireThat(tokensPerPeriod, "tokensPerPeriod").isPositive();
			ensureOpen();
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
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public Duration period()
		{
			ensureOpen();
			return period;
		}

		/**
		 * Indicates how often {@code tokensPerPeriod} should be added to the bucket.
		 *
		 * @param period indicates how often {@code tokensPerPeriod} should be added to the bucket
		 * @return this
		 * @throws NullPointerException  if {@code period} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater period(Duration period)
		{
			requireThat(period, "period").isGreaterThan(Duration.ZERO);
			ensureOpen();
			if (period.equals(this.period))
				return this;
			changed = true;
			this.period = period;
			return this;
		}

		/**
		 * Returns the number of available tokens. The value may be negative, in which case the bucket must
		 * accumulate a positive number of tokens before they may be consumed.
		 *
		 * @return the number of available tokens
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public long availableTokens()
		{
			ensureOpen();
			return availableTokens;
		}

		/**
		 * Sets the number of available tokens. The value may be negative, in which case the bucket must
		 * accumulate a positive number of tokens before they may be consumed.
		 *
		 * @param availableTokens the number of available tokens
		 * @return this
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater availableTokens(long availableTokens)
		{
			ensureOpen();
			if (availableTokens == this.availableTokens)
				return this;
			changed = true;
			this.availableTokens = availableTokens;
			return this;
		}

		/**
		 * Returns the number of tokens that are refilled at a time.
		 * <p>
		 * At one end of the spectrum, one may refill tokens one at a time during the {@link #getPeriod() period}.
		 * On the other spectrum, one may refill {@code tokensPerPeriod} tokens once per {@code period}.
		 * <p>
		 * <b>NOTE</b>: When completing a {@code period} a smaller number of tokens may be refilled in order to
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod} due to rounding errors.
		 *
		 * @return the minimum number of tokens by which the limit may be refilled
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public long refillSize()
		{
			ensureOpen();
			return refillSize;
		}

		/**
		 * Sets the number of tokens that are refilled at a time.
		 * <p>
		 * At one end of the spectrum, one may refill tokens one at a time during the {@link #getPeriod() period}.
		 * On the other spectrum, one may refill {@code tokensPerPeriod} tokens once per {@code period}.
		 * <p>
		 * <b>NOTE</b>: When completing a {@code period} a smaller number of tokens may be refilled in order to
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod} due to rounding errors.
		 *
		 * @param refillSize the minimum number of tokens by which the limit may be refilled
		 * @return this
		 * @throws IllegalArgumentException if {@code refillSize} is negative or zero
		 * @throws IllegalStateException    if the updater is closed
		 */
		public ConfigurationUpdater refillSize(long refillSize)
		{
			requireThat(refillSize, "refillSize").isPositive();
			ensureOpen();
			if (refillSize == this.refillSize)
				return this;
			changed = true;
			this.refillSize = refillSize;
			return this;
		}

		/**
		 * Returns the maximum number of tokens that the bucket may hold before overflowing.
		 *
		 * @return the maximum number of tokens that the bucket may hold before overflowing (subsequent tokens
		 * are discarded)
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public long maximumTokens()
		{
			ensureOpen();
			return maximumTokens;
		}

		/**
		 * Sets the maximum number of tokens that the bucket may hold before overflowing.
		 *
		 * @param maximumTokens the maximum number of tokens that the bucket may hold before overflowing
		 *                      (subsequent tokens are discarded)
		 * @return this
		 * @throws IllegalArgumentException if {@code maximumTokens} is negative or zero
		 * @throws IllegalStateException    if the updater is closed
		 */
		public ConfigurationUpdater maximumTokens(long maximumTokens)
		{
			requireThat(maximumTokens, "maximumTokens").isPositive();
			ensureOpen();
			if (maximumTokens == this.maximumTokens)
				return this;
			changed = true;
			this.maximumTokens = maximumTokens;
			return this;
		}

		/**
		 * Returns the data associated with this limit.
		 *
		 * @return the data associated with this limit
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public Object userData()
		{
			ensureOpen();
			return userData;
		}

		/**
		 * Sets the data associated with this limit.
		 *
		 * @param userData the data associated with this limit
		 * @return this
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater userData(Object userData)
		{
			ensureOpen();
			if (Objects.equals(userData, this.userData))
				return this;
			changed = true;
			this.userData = userData;
			return this;
		}

		/**
		 * @throws IllegalStateException if the updater is closed
		 */
		private void ensureOpen()
		{
			if (closed)
				throw new IllegalStateException("ConfigurationUpdater is closed");
		}

		/**
		 * Updates this Limit's configuration and releases its lock.
		 * <p>
		 * A new period is started immediately after the update takes place.
		 */
		@Override
		public void close()
		{
			if (closed)
				return;
			closed = true;
			if (!changed)
				return;
			log.debug("Before updating limit: {}", Limit.this);

			try
			{
				Limit.this.tokensPerPeriod = tokensPerPeriod;
				Limit.this.period = period;
				// availableTokens takes the place of initialTokens (which would be meaningless to update)
				Limit.this.availableTokens = availableTokens;
				Limit.this.maximumTokens = maximumTokens;
				Limit.this.refillSize = refillSize;
				Limit.this.userData = userData;
				Limit.this.startOfCurrentPeriod = Instant.now();
				Limit.this.tokensAddedInCurrentPeriod = 0;
				Limit.this.refillsElapsed = 0;
				Limit.this.timePerToken = period.dividedBy(tokensPerPeriod);
				Limit.this.timePerRefill = timePerToken.multipliedBy(refillSize);
				Limit.this.refillsPerPeriod = (long) Math.ceil((double) tokensPerPeriod / refillSize);
				Limit.this.nextRefillAt = getRefillEndTime(1);
				overflowBucket();
			}
			finally
			{
				writeLock.close();
			}
			log.debug("After updating limit: {}", Limit.this);
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder(ConfigurationUpdater.class).
				add("tokensPerPeriod", tokensPerPeriod).
				add("period", period).
				add("availableTokens", availableTokens).
				add("maximumTokens", maximumTokens).
				add("refillSize", refillSize).
				add("userData", userData).
				add("changed", changed).
				toString();
		}
	}
}