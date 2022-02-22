package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.AbstractContainer;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.ReadWriteLockAsResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A container with Limits.
 *
 * <b>Thread safety</b>: This class is thread-safe.
 */
public final class Bucket extends AbstractContainer
{
	private final Logger log = LoggerFactory.getLogger(Bucket.class);
	private Set<Limit> limits;

	/**
	 * Builds a new bucket.
	 *
	 * @return a Bucket builder
	 */
	public static Builder builder()
	{
		return new Builder(new ReadWriteLockAsResource(), bucket ->
		{
		});
	}

	/**
	 * Creates a child bucket.
	 *
	 * @param lock     the lock over the bucket's state
	 * @param userData the data associated with this bucket
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code limits} is empty
	 */
	private Bucket(Set<Limit> limits, Object userData, ReadWriteLockAsResource lock)
	{
		super(lock, Bucket::tryConsumeRange);
		requireThat(limits, "limits").isNotEmpty();
		this.limits = Set.copyOf(limits);
		this.userData = userData;
	}

	@Override
	protected long getTokensAvailable()
	{
		if (limits.isEmpty())
			return 0;
		long tokensAvailable = Long.MAX_VALUE;
		for (Limit limit : limits)
			tokensAvailable = Math.min(tokensAvailable, limit.getTokensAvailable());
		return tokensAvailable;
	}

	@Override
	protected void updateChild(Object child, Runnable update)
	{
		Limit limit = (Limit) child;
		Set<Limit> newLimits = new HashSet<>(limits);
		newLimits.remove(limit);

		update.run();

		newLimits.add(limit);
		limits = Set.copyOf(newLimits);
	}

	@Override
	protected Logger getLogger()
	{
		return log;
	}

	/**
	 * Returns the limits associated with this bucket.
	 *
	 * @return an unmodifiable set
	 */
	public Set<Limit> getLimits()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return limits;
		}
	}

	/**
	 * Adds tokens to the bucket without surpassing any limit's {@code maxCapacity}. The refill rate is not
	 * impacted.
	 *
	 * @param tokens the number of tokens to add to the bucket
	 * @return the number of tokens that were added
	 * @throws IllegalArgumentException if {@code tokens} is negative
	 */
	@CheckReturnValue
	public long addTokens(long tokens)
	{
		requireThat(tokens, "tokens").isNotNegative();
		long spaceLeft = tokens;
		try (CloseableLock ignored = lock.writeLock())
		{
			for (Limit limit : limits)
				spaceLeft = Math.min(spaceLeft, limit.getSpaceLeft());
			if (spaceLeft > 0)
			{
				for (Limit limit : limits)
					limit.addTokens(spaceLeft);
				tokensUpdated.signalAll();
			}
		}
		return spaceLeft;
	}

	/**
	 * Attempts to consume {@code [minimumTokens, maximumTokens]}. Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokens  the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens  the maximum  number of tokens to consume (inclusive)
	 * @param requestedAt    the time at which the tokens were requested
	 * @param abstractBucket the container
	 * @return the minimum amount of time until the requested number of tokens will be available
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maxTokens} that is less than {@code minimumTokensRequested}.
	 */
	private static ConsumptionResult tryConsumeRange(long minimumTokens, long maximumTokens,
	                                                 Instant requestedAt, AbstractContainer abstractBucket)
	{
		assertThat(minimumTokens, "minimumTokens").isNotNegative();
		assertThat(maximumTokens, "maximumTokens").isNotNegative().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		assertThat(requestedAt, "requestedAt").isNotNull();
		Bucket bucket = (Bucket) abstractBucket;
		try (CloseableLock ignored = bucket.lock.writeLock())
		{
			Set<Limit> limits = bucket.getLimits();
			long tokensAdded = 0;
			for (Limit limit : limits)
				tokensAdded = Math.max(tokensAdded, limit.refill(requestedAt));
			if (tokensAdded > 0)
				bucket.tokensUpdated.signalAll();
			long tokensConsumed = 0;
			long tokensAvailable = Long.MAX_VALUE;
			ConsumptionSimulation longestDelay = new ConsumptionSimulation(tokensConsumed, 0, requestedAt,
				requestedAt);
			Comparator<Duration> comparator = Comparator.naturalOrder();
			for (Limit limit : limits)
			{
				ConsumptionSimulation newConsumption = bucket.simulateConsumption(minimumTokens, maximumTokens, limit,
					requestedAt);
				tokensAvailable = Math.min(tokensAvailable, bucket.getTokensAvailable());
				tokensConsumed = Math.max(tokensConsumed, newConsumption.getTokensConsumed());
				if (comparator.compare(newConsumption.getTokensAvailableIn(),
					longestDelay.getTokensAvailableIn()) > 0)
				{
					longestDelay = newConsumption;
				}
			}
			if (tokensConsumed > 0)
			{
				// Success
				for (Limit limit : limits)
					limit.consume(tokensConsumed);
				bucket.tokensUpdated.signalAll();
				return new ConsumptionResult(bucket, minimumTokens, maximumTokens, tokensConsumed, requestedAt,
					requestedAt);
			}
			assertThat(longestDelay.getTokensConsumed(), "longestDelay.getTokensConsumed()").isZero();
			return new ConsumptionResult(bucket, minimumTokens, maximumTokens, tokensConsumed, requestedAt,
				longestDelay.getTokensAvailableAt());
		}
	}

	/**
	 * Simulates consumption with respect to a single limit.
	 *
	 * @param minimumTokens the minimum number of tokens that were requested
	 * @param maximumTokens the maximum number of tokens that were requested
	 * @param limit         the limit
	 * @param requestedAt   the time at which the tokens were requested
	 * @return the simulated consumption
	 * @throws IllegalArgumentException if the limit has a {@code maxTokens} that is less than
	 *                                  {@code minimumTokensRequested}
	 */
	private ConsumptionSimulation simulateConsumption(long minimumTokens, long maximumTokens, Limit limit,
	                                                  Instant requestedAt)
	{
		requireThat(limit.getMaxTokens(), "limit.getMaxTokens()").
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		long tokensAvailable = limit.getTokensAvailable();
		Instant tokensAvailableAt;
		long tokensConsumed;
		if (tokensAvailable < minimumTokens)
		{
			long tokensNeeded = minimumTokens - tokensAvailable;
			long periodsToSleep = tokensNeeded / limit.getTokensPerPeriod();
			Instant lastRefillTime = limit.getLastRefilledAt();
			Duration period = limit.getPeriod();
			tokensAvailableAt = lastRefillTime.plus(period.multipliedBy(periodsToSleep));
			tokensConsumed = 0;
		}
		else
		{
			tokensAvailableAt = requestedAt;
			tokensConsumed = Math.min(maximumTokens, tokensAvailable);
			tokensAvailable -= tokensConsumed;
		}
		return new ConsumptionSimulation(tokensConsumed, tokensAvailable, requestedAt, tokensAvailableAt);
	}

	/**
	 * Updates this Bucket's configuration.
	 * <p>
	 * Please note that users are allowed to consume tokens between the time this method is invoked and
	 * {@link ConfigurationUpdater#apply()} completes. Users who wish to add/remove a relative amount of
	 * tokens should avoid accessing the bucket or its limits until the configuration update is complete.
	 *
	 * @return the configuration updater
	 */
	public ConfigurationUpdater updateConfiguration()
	{
		return new ConfigurationUpdater();
	}

	// Export Javadoc without exporting AbstractContainer
	@Override
	@CheckReturnValue
	public Object getUserData()
	{
		return super.getUserData();
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume()
	{
		return super.tryConsume();
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens)
	{
		return super.tryConsume(tokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsumeRange(long minimumTokens, long maximumTokens)
	{
		return super.tryConsumeRange(minimumTokens, maximumTokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consume() throws InterruptedException
	{
		return super.consume();
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consume(long tokens) throws InterruptedException
	{
		return super.consume(tokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consume(long tokens, long timeout, TimeUnit unit) throws InterruptedException
	{
		return super.consume(tokens, timeout, unit);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consumeRange(long minimumTokens, long maximumTokens) throws InterruptedException
	{
		return super.consumeRange(minimumTokens, maximumTokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consumeRange(long minimumTokens, long maximumTokens, long timeout, TimeUnit unit)
		throws InterruptedException
	{
		return super.consumeRange(minimumTokens, maximumTokens, timeout, unit);
	}

	@Override
	public String toString()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return "limits: " + limits + ", userData: " + userData;
		}
	}

	/**
	 * The result of a simulated token consumption.
	 */
	@SuppressWarnings("ClassCanBeRecord")
	private static final class ConsumptionSimulation
	{
		private final long tokensConsumed;
		private final long tokensAvailable;
		private final Instant requestedAt;
		private final Instant tokensAvailableAt;

		/**
		 * Creates the result of a simulated token consumption.
		 *
		 * @param tokensConsumed    the number of tokens that were consumed
		 * @param tokensAvailable   the number of tokens that were left for consumption
		 * @param requestedAt       the time at which the tokens were requested
		 * @param tokensAvailableAt the time at which the requested tokens will become available
		 * @throws NullPointerException     if any of the arguments are null
		 * @throws IllegalArgumentException if any of the arguments are negative. If
		 *                                  {@code minimumTokensRequested > maximumTokensRequested}. If
		 *                                  {@code tokensConsumed > 0 && tokensConsumed < minimumTokensRequested}
		 */
		private ConsumptionSimulation(long tokensConsumed, long tokensAvailable, Instant requestedAt,
		                              Instant tokensAvailableAt)
		{
			assertThat(tokensConsumed, "tokensConsumed").isNotNegative();
			assertThat(tokensAvailable, "tokensLeft").isNotNegative();
			assertThat(requestedAt, "requestedAt").isNotNull();
			assertThat(tokensAvailableAt, "tokensAvailableAt").isNotNull();
			this.tokensConsumed = tokensConsumed;
			this.tokensAvailable = tokensAvailable;
			this.requestedAt = requestedAt;
			this.tokensAvailableAt = tokensAvailableAt;
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
			return tokensConsumed >= 0;
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
		public String toString()
		{
			return "successful: " + isSuccessful() + ", tokensConsumed: " + tokensConsumed + ", tokensLeft: " +
				tokensAvailable + ", requestedAt: " + requestedAt + ", tokensAvailableAt: " + tokensAvailableAt;
		}
	}

	/**
	 * Builds a bucket.
	 */
	public static final class Builder
	{
		private final ReadWriteLockAsResource lock;
		private final Consumer<Bucket> consumer;
		private final Set<Limit> limits = new HashSet<>();
		private Object userData;

		/**
		 * Builds a bucket.
		 *
		 * @param lock     the lock over the bucket's state
		 * @param consumer consumes the bucket before it is returned
		 * @throws NullPointerException if any of the arguments are null
		 */
		Builder(ReadWriteLockAsResource lock, Consumer<Bucket> consumer)
		{
			assertThat(lock, "lock").isNotNull();
			assertThat(consumer, "consumer").isNotNull();
			this.lock = lock;
			this.consumer = consumer;
		}

		/**
		 * Adds a limit that the bucket must respect.
		 *
		 * @param limitBuilder builds the Limit
		 * @return this
		 * @throws IllegalArgumentException if {@code tokensPerPeriod} or {@code period} are negative or zero
		 * @throws NullPointerException     if any of the arguments are null
		 */
		@CheckReturnValue
		public Builder addLimit(Consumer<Limit.Builder> limitBuilder)
		{
			requireThat(limitBuilder, "limitBuilder").isNotNull();
			limitBuilder.accept(new Limit.Builder(limits::add));
			return this;
		}

		/**
		 * Sets user data associated with this bucket.
		 *
		 * @param userData the data associated with this bucket
		 * @return this
		 */
		@CheckReturnValue
		public Builder userData(Object userData)
		{
			this.userData = userData;
			return this;
		}

		/**
		 * Builds a new Bucket.
		 *
		 * @return a new Bucket
		 */
		public Bucket build()
		{
			// Locking because of https://stackoverflow.com/a/41990379/14731
			try (CloseableLock ignored = lock.writeLock())
			{
				Bucket bucket = new Bucket(limits, userData, lock);
				for (Limit limit : limits)
				{
					limit.parent = bucket::updateChild;
					limit.lock = bucket.lock;
				}
				consumer.accept(bucket);
				return bucket;
			}
		}
	}

	/**
	 * Updates this Bucket's configuration.
	 * <p>
	 * <b>Thread-safety</b>: This class is not thread-safe.
	 */
	public final class ConfigurationUpdater
	{
		private final Set<Limit> limits;
		private Object userData;
		private boolean changed;
		private boolean wakeConsumers;

		/**
		 * Creates a new configuration updater.
		 */
		ConfigurationUpdater()
		{
			try (CloseableLock ignored = lock.readLock())
			{
				this.limits = new HashSet<>(Bucket.this.limits);
				this.userData = Bucket.this.userData;
			}
		}

		/**
		 * Adds a limit that the bucket must respect.
		 *
		 * @param limitBuilder builds a Limit
		 * @return this
		 * @throws NullPointerException if {@code limit} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater addLimit(Consumer<Limit.Builder> limitBuilder)
		{
			requireThat(limitBuilder, "limitBuilder").isNotNull();
			// Adding a limit causes consumers to have to wait the same amount of time, or longer. No need to
			// wake sleeping consumers.
			limitBuilder.accept(new Limit.Builder(e ->
			{
				if (limits.add(e))
					changed = true;
			}));
			return this;
		}

		/**
		 * Removes a limit that the bucket must respect.
		 *
		 * @param limit a limit
		 * @return this
		 * @throws NullPointerException if {@code limit} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater removeLimit(Limit limit)
		{
			requireThat(limit, "limit").isNotNull();
			if (limits.remove(limit))
			{
				changed = true;
				wakeConsumers = true;
			}
			return this;
		}

		/**
		 * Removes all limits.
		 *
		 * @return this
		 */
		@CheckReturnValue
		public ConfigurationUpdater clear()
		{
			if (this.limits.isEmpty())
				return this;
			changed = true;
			limits.clear();
			return this;
		}

		/**
		 * Sets user data associated with this bucket.
		 *
		 * @param userData the data associated with this bucket
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
		 * Updates this Bucket's configuration.
		 *
		 * @throws IllegalArgumentException if {@code limits} is empty
		 */
		public void apply()
		{
			requireThat(limits, "limits").isNotEmpty();
			if (!changed)
				return;
			try (CloseableLock ignored = lock.writeLock())
			{
				parent.updateChild(Bucket.this, () ->
				{
					Bucket.this.limits = Set.copyOf(limits);
					Bucket.this.userData = userData;
				});
				if (wakeConsumers)
					tokensUpdated.signalAll();
			}
		}
	}
}