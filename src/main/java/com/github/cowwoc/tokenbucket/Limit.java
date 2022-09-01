package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.Requirements;
import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.ReadWriteLockAsResource;
import com.github.cowwoc.tokenbucket.internal.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.assertionsAreEnabled;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A rate limit.
 */
public final class Limit
{
	Bucket bucket;
	ReadWriteLockAsResource lock;

	long tokensPerPeriod;
	Duration period;
	private final long initialTokens;
	long maximumTokens;
	long refillSize;
	private Object userData;
	private boolean userDataInToString;
	Instant startOfCurrentPeriod;
	long tokensAddedInCurrentPeriod;
	Instant nextRefillAt;
	long availableTokens;
	long refillsElapsed;
	Duration timePerToken;
	Duration timePerRefill;
	long refillsPerPeriod;
	private final Requirements requirements = new Requirements();
	private final Logger log = LoggerFactory.getLogger(Limit.class);

	/**
	 * Adds a limit that the bucket must respect.
	 * <p>
	 * By default,
	 * <ul>
	 * <li>{@code tokensPerPeriod} is 1.</li>
	 * <li>{@code period} is 1 second.</li>
	 * <li>{@code initialTokens} is 0.</li>
	 * <li>{@code maximumTokens} is {@code Long.MAX_VALUE}.</li>
	 * <li>{@code refillSize} is 1.</li>
	 * <li>{@code userData} is {@code null}.</li>
	 * <li>{@code userDataInToString} is {@code false}.</li>
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
	 * @param tokensPerPeriod    the amount of tokens to add to the bucket every {@code period}
	 * @param period             indicates how often {@code tokensPerPeriod} should be added to the bucket
	 * @param initialTokens      the initial amount of tokens in the bucket
	 * @param maximumTokens      the maximum amount of tokens that the bucket may hold before overflowing
	 *                           (subsequent tokens are discarded)
	 * @param refillSize         the number of tokens that are refilled at a time
	 * @param userData           the data associated with this limit
	 * @param userDataInToString true if {@code userData} should be included in {@link #toString()}
	 * @throws NullPointerException     if {@code period} is null
	 * @throws IllegalArgumentException if {@code initialTokens > maximumTokens} or
	 *                                  {@code tokensPerPeriod > maximumTokens}
	 */
	private Limit(long tokensPerPeriod, Duration period, long initialTokens, long maximumTokens,
	              long refillSize, Object userData, boolean userDataInToString)
	{
		// Assume that all other preconditions are enforced by Builder
		requirements.requireThat(maximumTokens, "maximumTokens").
			isGreaterThanOrEqualTo(tokensPerPeriod, "tokensPerPeriod").
			isGreaterThanOrEqualTo(initialTokens, "initialTokens");
		this.tokensPerPeriod = tokensPerPeriod;
		this.period = period;
		this.initialTokens = initialTokens;
		this.maximumTokens = maximumTokens;
		this.refillSize = refillSize;
		this.userData = userData;
		this.userDataInToString = userDataInToString;

		this.availableTokens = initialTokens;
		this.timePerToken = period.dividedBy(tokensPerPeriod);
		this.timePerRefill = timePerToken.multipliedBy(refillSize);
		this.startOfCurrentPeriod = Instant.now();
		this.tokensAddedInCurrentPeriod = 0;
		this.refillsElapsed = 0;
		this.refillsPerPeriod = (long) Math.ceil((double) tokensPerPeriod / refillSize);
		this.nextRefillAt = getRefillAt(1);
	}

	/**
	 * Returns the bucket containing this limit.
	 *
	 * @return the bucket containing this limit
	 */
	public Bucket getBucket()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return bucket;
		}
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
	 * Returns the number of tokens that are refilled at a time.
	 * <p>
	 * At one end of the spectrum, one may refill tokens one at a time during the {@link #getPeriod() period}.
	 * On the other spectrum, one may refill all tokens at the end of the {@code period}.
	 * <p>
	 * <b>NOTE</b>: When completing a {@code period}, a smaller number of tokens may be refilled in order to
	 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod}.
	 *
	 * @return the minimum number of tokens by which the limit may be refilled
	 */
	public long getRefillSize()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return refillSize;
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
	 * Indicates if {@code userData} is included in {@link #toString()}.
	 *
	 * @return true if {@code userData} is included in {@link #toString()}
	 * @throws IllegalStateException if the updater is closed
	 */
	public boolean isUserDataInToString()
	{
		return userDataInToString;
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
	 */
	void refill(Instant consumedAt)
	{
		if (!readyForRefill(consumedAt))
			return;
		if (requirements.assertionsAreEnabled())
		{
			requirements.withoutAnyContext().
				withContext("nextRefillAt", nextRefillAt).
				withContext("startOfCurrentPeriod", startOfCurrentPeriod).
				withContext("consumedAt", consumedAt);
		}
		Duration timeElapsedSinceStartOfPeriod = Duration.between(startOfCurrentPeriod, consumedAt);
		if (requirements.assertionsAreEnabled())
		{
			requirements.requireThat(timeElapsedSinceStartOfPeriod, "timeElapsedSinceStartOfPeriod").
				isGreaterThan(Duration.ZERO);
			requirements.requireThat(tokensAddedInCurrentPeriod, "tokensAddedInCurrentPeriod").
				isLessThanOrEqualTo(tokensPerPeriod, "tokensPerPeriod");
		}
		long tokensToAdd = 0;
		long numberOfPeriodsElapsed = timeElapsedSinceStartOfPeriod.dividedBy(period);
		if (numberOfPeriodsElapsed > 0)
		{
			if (requirements.assertionsAreEnabled())
			{
				requirements.
					withContext("numberOfPeriodsElapsed", numberOfPeriodsElapsed).
					withContext("tokensPerPeriod", tokensPerPeriod).
					withContext("tokensAddedInCurrentPeriod", tokensAddedInCurrentPeriod);
			}
			tokensToAdd += numberOfPeriodsElapsed * tokensPerPeriod - tokensAddedInCurrentPeriod;
			if (requirements.assertionsAreEnabled())
			{
				requirements.requireThat(tokensToAdd, "tokensToAdd").isNotNegative();
				requirements.withContext("tokensToAdd", tokensToAdd);
			}
			tokensAddedInCurrentPeriod = 0;
			startOfCurrentPeriod = startOfCurrentPeriod.plus(period.multipliedBy(numberOfPeriodsElapsed));
		}
		Duration timeElapsedInPeriod = timeElapsedSinceStartOfPeriod.
			minus(period.multipliedBy(numberOfPeriodsElapsed));
		refillsElapsed = timeElapsedInPeriod.dividedBy(timePerRefill);
		requirements.assertThat(timeElapsedInPeriod, "timeElapsedInPeriod").
			isBetweenClosed(timePerRefill.multipliedBy(refillsElapsed),
				timePerRefill.multipliedBy(refillsElapsed + 1));
		if (!timeElapsedInPeriod.isZero())
		{
			long tokensToAddInPeriod = refillSize * refillsElapsed - tokensAddedInCurrentPeriod;
			if (requirements.assertionsAreEnabled())
			{
				requirements.
					withContext("secondsPerToken", timePerToken).
					withContext("period", period).
					withContext("tokensPerPeriod", tokensPerPeriod).
					withContext("timePerRefill", timePerRefill).
					withContext("refillSize", refillSize).
					withContext("refillsElapsed", refillsElapsed).
					withContext("timeElapsedInPeriod", timeElapsedInPeriod).
					withContext("tokensAddedInCurrentPeriod", tokensAddedInCurrentPeriod);
				requirements.requireThat(tokensToAddInPeriod, "tokensToAddInPeriod").isNotNegative();
				requirements.withContext("tokensToAddInPeriod", tokensToAddInPeriod);
			}
			tokensToAdd += tokensToAddInPeriod;
			tokensAddedInCurrentPeriod += tokensToAddInPeriod;
		}
		requirements.assertThat(tokensToAdd, "tokensToAdd").isNotNegative();

		nextRefillAt = getRefillAt(refillsElapsed + 1);

		availableTokens = saturatedAdd(availableTokens, tokensToAdd);
		overflowBucket();
	}

	/**
	 * @param refills the desired number of refills
	 * @return the time at which the desired number of refills will take place
	 */
	private Instant getRefillAt(long refills)
	{
		Duration timeElapsedInPeriod = timePerRefill.multipliedBy(tokensAddedInCurrentPeriod / tokensPerPeriod);
		Instant result = this.startOfCurrentPeriod.plus(timeElapsedInPeriod);
		long numberOfPeriodsElapsed = refills / refillsPerPeriod;
		if (numberOfPeriodsElapsed > 0)
		{
			refills -= numberOfPeriodsElapsed * refillsPerPeriod;
			result = result.plus(period.multipliedBy(numberOfPeriodsElapsed));
		}

		long refillsLeftInPeriod = refillsPerPeriod - refillsElapsed;
		if (refills > refillsLeftInPeriod)
		{
			refills -= refillsLeftInPeriod;
			result = result.plus(timePerRefill.multipliedBy(refillsLeftInPeriod));
		}
		Duration timeAddedInPeriod = timePerRefill.multipliedBy(refills);
		return result.plus(timeAddedInPeriod);
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
	 * @param consumedAt    the time at which an attempt was made to consume tokens
	 * @return the simulated consumption
	 * @throws IllegalArgumentException if the limit has a {@code maximumTokens} that is less than
	 *                                  {@code minimumTokens}
	 */
	ConsumptionSimulation simulateConsumption(long minimumTokens, long maximumTokens, Instant consumedAt)
	{
		if (assertionsAreEnabled())
		{
			assertThat(minimumTokens, "minimumTokens").isPositive();
			assertThat(maximumTokens, "maximumTokens").isPositive().
				isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		}
		Instant availableAt;
		long tokensConsumed;
		if (availableTokens < minimumTokens)
		{
			long tokensNeeded = minimumTokens - availableTokens;
			long refillsNeeded = (long) Math.ceil((double) tokensNeeded / refillSize);
			availableAt = getRefillAt(refillsElapsed + refillsNeeded);
			tokensConsumed = 0;
		}
		else
		{
			availableAt = consumedAt;
			tokensConsumed = Math.min(maximumTokens, availableTokens);
			assertThat(tokensConsumed, "tokensConsumed()").isPositive();
		}
		return new ConsumptionSimulation(tokensConsumed, consumedAt, availableAt);
	}

	/**
	 * Updates this Limit's configuration.
	 * <p>
	 * The Limit and any attached Containers will be locked until {@link ConfigurationUpdater#close()}
	 * is invoked, at which point a new period will be started.
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
		return Objects.hash(tokensPerPeriod, period, initialTokens, maximumTokens, refillSize);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof Limit other)) return false;
		return tokensPerPeriod == other.tokensPerPeriod && initialTokens == other.initialTokens &&
			maximumTokens == other.maximumTokens && period.equals(other.period) &&
			refillSize == other.refillSize;
	}

	@Override
	public String toString()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			ToStringBuilder builder = new ToStringBuilder(Limit.class).
				add("availableTokens", availableTokens).
				add("tokensPerPeriod", tokensPerPeriod).
				add("period", period).
				add("maximumTokens", maximumTokens).
				add("refillSize", refillSize).
				add("refillsElapsed", refillsElapsed).
				add("nextRefillAt", nextRefillAt).
				add("initialTokens", initialTokens);
			if (userDataInToString)
				builder.add("userData", userData);
			return builder.toString();
		}
	}

	/**
	 * The result of a simulated token consumption.
	 */
	static final class ConsumptionSimulation
	{
		private final long tokensConsumed;
		private final Instant consumedAt;
		private final Instant availableAt;

		/**
		 * Creates the result of a simulated token consumption.
		 *
		 * @param tokensConsumed the number of tokens that would be consumed
		 * @param consumedAt     the time at which an attempt was made to consume tokens
		 * @param availableAt    the time at which the tokens will become available
		 * @throws NullPointerException     if any of the arguments are null
		 * @throws IllegalArgumentException if {@code tokensConsumed} is negative.
		 *                                  If {@code consumedAt > availableAt}.
		 */
		ConsumptionSimulation(long tokensConsumed, Instant consumedAt, Instant availableAt)
		{
			if (assertionsAreEnabled())
			{
				requireThat(tokensConsumed, "tokensConsumed").isNotNegative();
				requireThat(consumedAt, "consumedAt").isNotNull();
				requireThat(availableAt, "availableAt").isGreaterThanOrEqualTo(consumedAt, "consumedAt");
			}
			this.tokensConsumed = tokensConsumed;
			this.consumedAt = consumedAt;
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
			return Duration.between(consumedAt, availableAt);
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder().
				add("successful", isSuccessful()).
				add("tokensConsumed", tokensConsumed).
				add("consumedAt", consumedAt).
				add("availableAt", availableAt).
				toString();
		}
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
		private long refillSize = 1;
		private Object userData;
		private boolean userDataInToString;

		/**
		 * Adds a limit that the bucket must respect.
		 * <p>
		 * By default,
		 * <ul>
		 * <li>{@code tokensPerPeriod} is 1.</li>
		 * <li>{@code period} is 1 second.</li>
		 * <li>{@code initialTokens} is 0.</li>
		 * <li>{@code maximumTokens} is {@code Long.MAX_VALUE}.</li>
		 * <li>{@code refillSize} is 1.</li>
		 * <li>{@code userData} is {@code null}.</li>
		 * <li>{@code userDataInToString} is {@code false}.</li>
		 * </ul>
		 *
		 * @param consumer consumes the bucket before it is returned to the user
		 * @throws NullPointerException if {@code consumer} is null
		 */
		Builder(Consumer<Limit> consumer)
		{
			assertThat(consumer, "consumer").isNotNull();
			this.consumer = consumer;
		}

		/**
		 * Returns the amount of tokens to add to the bucket every {@code period}. The default is {@code 1}.
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
		 * Returns the initial amount of tokens in the bucket. The value may be negative, in which case the
		 * bucket must accumulate a positive number of tokens before they may be consumed. The default is
		 * {@code 0}.
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
		 * Returns the maximum amount of tokens that the bucket may hold before overflowing. The default is
		 * {@code Long.MAX_VALUE}.
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
		 * Returns the number of tokens that are refilled at a time.
		 * <p>
		 * At one end of the spectrum, one may refill tokens one at a time during the {@link #getPeriod() period}.
		 * On the other spectrum, one may refill {@code tokensPerPeriod} tokens once per {@code period}.
		 * <p>
		 * <b>NOTE</b>: When completing a {@code period} a smaller number of tokens may be refilled in order to
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod}.
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
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod}.
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
		 * Indicates if {@code userData} should be included in {@link #toString()}.
		 *
		 * @return true if {@code userData} should be included in {@link #toString()}
		 */
		@CheckReturnValue
		public boolean userDataInToString()
		{
			return userDataInToString;
		}

		/**
		 * Indicates if {@code userData} should be included in {@link #toString()}.
		 *
		 * @param userDataInToString the data associated with this limit
		 * @return this
		 */
		public Builder userDataInString(boolean userDataInToString)
		{
			this.userDataInToString = userDataInToString;
			return this;
		}

		/**
		 * Builds a new Limit.
		 *
		 * @return a new Limit
		 */
		public Limit build()
		{
			Limit limit = new Limit(tokensPerPeriod, period, initialTokens, maximumTokens, refillSize, userData,
				userDataInToString);
			consumer.accept(limit);
			return limit;
		}

		@Override
		public String toString()
		{
			ToStringBuilder builder = new ToStringBuilder(Builder.class).
				add("tokensPerPeriod", tokensPerPeriod).
				add("period", period).
				add("refillSize", refillSize).
				add("maximumTokens", maximumTokens);
			if (userDataInToString)
				builder.add("userData", userData);
			return builder.
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
		private boolean userDataInToString;
		private boolean changed;

		/**
		 * Creates a new configuration updater.
		 */
		private ConfigurationUpdater()
		{
			this.tokensPerPeriod = Limit.this.tokensPerPeriod;
			this.period = Limit.this.period;
			this.availableTokens = Limit.this.availableTokens;
			this.refillSize = Limit.this.refillSize;
			this.maximumTokens = Limit.this.maximumTokens;
			this.userData = Limit.this.userData;
			this.userDataInToString = Limit.this.userDataInToString;
		}

		/**
		 * Returns the amount of tokens to add to the bucket every {@code period}.
		 *
		 * @return the amount of tokens to add to the bucket every {@code period}
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public long tokensPerPeriod()
		{
			ensureOpen();
			return tokensPerPeriod;
		}

		/**
		 * Sets the amount of tokens to add to the bucket every {@code period}.
		 *
		 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
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
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod}.
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
		 * avoid surpassing {@link #getTokensPerPeriod() tokensPerPeriod}.
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
		 * Returns the maximum amount of tokens that the bucket may hold before overflowing.
		 *
		 * @return the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens
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
		 * Sets the maximum amount of tokens that the bucket may hold before overflowing.
		 *
		 * @param maximumTokens the maximum amount of tokens that the bucket may hold before overflowing
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
		 * Indicates if {@code userData} should be included in {@link #toString()}.
		 *
		 * @return true if {@code userData} should be included in {@link #toString()}
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public boolean userDataInToString()
		{
			ensureOpen();
			return userDataInToString;
		}

		/**
		 * Indicates if {@code userData} should be included in {@link #toString()}.
		 *
		 * @param userDataInToString the data associated with this limit
		 * @return this
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater userDataInString(boolean userDataInToString)
		{
			ensureOpen();
			if (userDataInToString == this.userDataInToString)
				return this;
			changed = true;
			this.userDataInToString = userDataInToString;
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
			try
			{
				if (!changed)
					return;
				log.debug("Before updating limit: {}", Limit.this);
				long availableTokens = this.availableTokens;
				Limit.this.tokensPerPeriod = tokensPerPeriod;
				if (Limit.this.tokensAddedInCurrentPeriod > Limit.this.tokensPerPeriod)
					Limit.this.tokensAddedInCurrentPeriod = Limit.this.tokensPerPeriod;
				Limit.this.period = period;
				// Do not change initialTokens
				Limit.this.maximumTokens = maximumTokens;
				Limit.this.refillSize = refillSize;
				Limit.this.userData = userData;
				Limit.this.userDataInToString = userDataInToString;
				Limit.this.availableTokens = availableTokens;
				Limit.this.nextRefillAt = Limit.this.startOfCurrentPeriod;
				overflowBucket();
				log.debug("After updating limit: {}", Limit.this);
			}
			finally
			{
				writeLock.close();
			}
		}

		@Override
		public String toString()
		{
			ToStringBuilder builder = new ToStringBuilder(ConfigurationUpdater.class).
				add("tokensPerPeriod", tokensPerPeriod).
				add("period", period).
				add("availableTokens", availableTokens).
				add("refillSize", refillSize).
				add("maximumTokens", maximumTokens);
			if (userDataInToString)
				builder.add("userData", userData);
			return builder.add("changed", changed).
				toString();
		}
	}
}