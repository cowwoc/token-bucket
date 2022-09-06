package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.Limit.SimulatedConsumption;
import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.AbstractContainer;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.ReadWriteLockAsResource;
import com.github.cowwoc.tokenbucket.internal.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.assertionsAreEnabled;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A container with Limits.
 * <p>
 * <b>Thread safety</b>: This class is thread-safe.
 */
public final class Bucket extends AbstractContainer
{
	private List<Limit> limits;
	private final Logger log = LoggerFactory.getLogger(Bucket.class);

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
	 * Creates a new bucket.
	 *
	 * @param limits    the limits associated with this bucket
	 * @param listeners the event listeners associated with this bucket
	 * @param userData  the data associated with this bucket
	 * @param lock      the lock over the bucket's state
	 * @throws NullPointerException     if {@code limits}, {@code listeners} or {@code lock} are null
	 * @throws IllegalArgumentException if {@code limits} is empty
	 */
	private Bucket(List<Limit> limits, List<ContainerListener> listeners, Object userData,
	               ReadWriteLockAsResource lock)
	{
		super(listeners, userData, Bucket::tryConsume, lock);
		assertThat(r -> r.requireThat(limits, "limits").isNotEmpty());
		this.limits = List.copyOf(limits);
	}

	@Override
	protected long getAvailableTokens()
	{
		long availableTokens = Long.MAX_VALUE;
		for (Limit limit : limits)
			availableTokens = Math.min(availableTokens, limit.availableTokens);
		return availableTokens;
	}

	@Override
	protected List<Limit> getLimitsWithInsufficientTokens(long tokens)
	{
		List<Limit> result = new ArrayList<>();
		for (Limit limit : limits)
			if (limit.availableTokens < tokens)
				result.add(limit);
		return result;
	}

	@Override
	protected long getMaximumTokens()
	{
		long maximumTokens = Long.MAX_VALUE;
		for (Limit limit : limits)
			maximumTokens = Math.min(maximumTokens, limit.getMaximumTokens());
		return maximumTokens;
	}

	@Override
	protected Logger getLogger()
	{
		return log;
	}

	/**
	 * Returns the limits associated with this bucket.
	 *
	 * @return an unmodifiable list
	 */
	public List<Limit> getLimits()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return limits;
		}
	}

	/**
	 * Returns the limit with the lowest refill rate.
	 *
	 * @return the limit with the lowest refill rate
	 */
	public Limit getLimitWithLowestRefillRate()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			Limit result = limits.get(0);
			double minimumTokensPerSecond = (double) result.getTokensPerPeriod() / result.getPeriod().toSeconds();
			for (int i = 1, size = limits.size(); i < size; ++i)
			{
				Limit limit = limits.get(i);
				double tokensPerSecond = (double) limit.getTokensPerPeriod() / limit.getPeriod().toSeconds();
				if (Double.compare(tokensPerSecond, minimumTokensPerSecond) < 0)
				{
					minimumTokensPerSecond = tokensPerSecond;
					result = limit;
				}
			}
			return result;
		}
	}

	/**
	 * Consumes {@code [minimumTokens, maximumTokens]} tokens, only if they are available at the time of
	 * invocation. Consumption order is not guaranteed to be fair.
	 *
	 * @param minimumTokens       the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens       the maximum  number of tokens to consume (inclusive)
	 * @param nameOfMinimumTokens the name of the {@code minimumTokens} parameter
	 * @param requestedAt         the time at which the tokens were requested
	 * @param consumedAt          the time at which an attempt was made to consume tokens
	 * @param abstractBucket      the container
	 * @return the result of the operation
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code nameOfMinimumTokens} is empty. If
	 *                                  {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maximumTokens} that is less than {@code minimumTokens}.
	 */
	private static ConsumptionResult tryConsume(long minimumTokens, long maximumTokens,
	                                            String nameOfMinimumTokens, Instant requestedAt,
	                                            Instant consumedAt, AbstractContainer abstractBucket)
	{
		if (assertionsAreEnabled())
		{
			requireThat(nameOfMinimumTokens, "nameOfMinimumTokens").isNotEmpty();
			requireThat(requestedAt, "requestedAt").isNotNull();
			requireThat(consumedAt, "consumedAt").isGreaterThanOrEqualTo(requestedAt, "requestedAt");
		}

		Bucket bucket = (Bucket) abstractBucket;
		List<Limit> limits = bucket.getLimits();
		for (Limit limit : limits)
		{
			requireThat(minimumTokens, nameOfMinimumTokens).
				isLessThanOrEqualTo(limit.getMaximumTokens(), "limit.getMaximumTokens()");
			limit.refill(consumedAt);
		}
		long tokensConsumed = Long.MAX_VALUE;
		Instant latestAvailableAt = consumedAt;
		Limit bottleneck = null;
		for (Limit limit : limits)
		{
			SimulatedConsumption simulatedConsumption = limit.simulateConsumption(minimumTokens, maximumTokens,
				consumedAt);
			tokensConsumed = Math.min(tokensConsumed, simulatedConsumption.getTokensConsumed());
			if (simulatedConsumption.getAvailableAt().compareTo(latestAvailableAt) > 0)
			{
				latestAvailableAt = simulatedConsumption.getAvailableAt();
				bottleneck = limit;
			}
		}
		if (tokensConsumed > 0)
		{
			// If there are any remaining tokens after consumption then wake up other consumers
			long minimumTokensLeft = Long.MAX_VALUE;
			for (Limit limit : limits)
			{
				long tokensLeft = limit.consume(tokensConsumed);
				if (tokensLeft < minimumTokensLeft)
					minimumTokensLeft = tokensLeft;
			}
			if (minimumTokensLeft > 0)
				bucket.tokensUpdated.signalAll();
			return new ConsumptionResult(bucket, minimumTokens, maximumTokens, tokensConsumed, requestedAt,
				consumedAt, consumedAt, minimumTokensLeft, List.of());
		}
		assert (bottleneck != null);
		return new ConsumptionResult(bucket, minimumTokens, maximumTokens, tokensConsumed, requestedAt,
			consumedAt, latestAvailableAt, 0, List.of(bottleneck));
	}

	/**
	 * Updates this Bucket's configuration.
	 * <p>
	 * The Bucket and any attached Limits, Containers will be locked until
	 * {@link ConfigurationUpdater#close()} is invoked.
	 *
	 * @return the configuration updater
	 */
	@CheckReturnValue
	public ConfigurationUpdater updateConfiguration()
	{
		return new ConfigurationUpdater();
	}

	// Export Javadoc without exporting AbstractContainer
	@Override
	public List<ContainerListener> getListeners()
	{
		return super.getListeners();
	}

	@Override
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

	@CheckReturnValue
	protected ConsumptionResult tryConsume(Instant consumedAt)
	{
		return super.tryConsume(consumedAt);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens)
	{
		return super.tryConsume(tokens);
	}

	@CheckReturnValue
	protected ConsumptionResult tryConsume(Instant consumedAt, long tokens)
	{
		return super.tryConsume(consumedAt, tokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens, long timeout, TimeUnit unit) throws InterruptedException
	{
		return super.tryConsume(tokens, timeout, unit);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long minimumTokens, long maximumTokens)
	{
		return super.tryConsume(minimumTokens, maximumTokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long minimumTokens, long maximumTokens, long timeout, TimeUnit unit)
		throws InterruptedException
	{
		return super.tryConsume(minimumTokens, maximumTokens, timeout, unit);
	}

	@Override
	public ConsumptionResult consume() throws InterruptedException
	{
		return super.consume();
	}

	@Override
	public ConsumptionResult consume(long tokens) throws InterruptedException
	{
		return super.consume(tokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consume(long minimumTokens, long maximumTokens) throws InterruptedException
	{
		return super.consume(minimumTokens, maximumTokens);
	}

	@Override
	public String toString()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return new ToStringBuilder(Bucket.class).
				add("limits", limits).
				add("userData", userData).
				toString();
		}
	}

	/**
	 * Builds a bucket.
	 */
	public static final class Builder
	{
		private final ReadWriteLockAsResource lock;
		private final Consumer<Bucket> consumer;
		private final List<Limit> limits = new ArrayList<>();
		private final List<ContainerListener> listeners = new ArrayList<>();
		private Object userData;

		/**
		 * Builds a bucket.
		 *
		 * @param lock     the lock over the bucket's state
		 * @param consumer consumes the bucket before it is returned to the user
		 * @throws NullPointerException if any of the arguments are null
		 */
		Builder(ReadWriteLockAsResource lock, Consumer<Bucket> consumer)
		{
			assertThat(r ->
			{
				r.requireThat(lock, "lock").isNotNull();
				r.requireThat(consumer, "consumer").isNotNull();
			});
			this.lock = lock;
			this.consumer = consumer;
		}

		/**
		 * Returns the limits that the bucket must respect.
		 *
		 * @return the limits that the bucket must respect
		 */
		@CheckReturnValue
		public List<Limit> limits()
		{
			return limits;
		}

		/**
		 * Adds a limit that the bucket must respect.
		 *
		 * @param limitBuilder builds the Limit
		 * @return this
		 * @throws NullPointerException if any of the arguments are null
		 */
		@CheckReturnValue
		public Builder addLimit(Consumer<Limit.Builder> limitBuilder)
		{
			requireThat(limitBuilder, "limitBuilder").isNotNull();
			limitBuilder.accept(new Limit.Builder(limits::add));
			return this;
		}

		/**
		 * Returns the event listeners associated with this bucket.
		 *
		 * @return this
		 */
		public List<ContainerListener> listeners()
		{
			return listeners;
		}

		/**
		 * Adds an event listener to the bucket.
		 *
		 * @param listener a listener
		 * @return this
		 * @throws NullPointerException if {@code listener} is null
		 */
		public Builder addListener(ContainerListener listener)
		{
			requireThat(listener, "listener").isNotNull();
			listeners.add(listener);
			return this;
		}

		/**
		 * Returns the user data associated with this bucket.
		 *
		 * @return the data associated with this bucket
		 */
		@CheckReturnValue
		public Object userData()
		{
			return userData;
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
				Bucket bucket = new Bucket(limits, listeners, userData, lock);
				for (Limit limit : limits)
				{
					limit.bucket = bucket;
					limit.lock = bucket.lock;
				}
				consumer.accept(bucket);
				return bucket;
			}
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder(Builder.class).
				add("limits", limits).
				add("userData", userData).
				toString();
		}
	}

	/**
	 * Updates this Bucket's configuration.
	 * <p>
	 * <b>Thread-safety</b>: This class is not thread-safe.
	 */
	public final class ConfigurationUpdater implements AutoCloseable
	{
		private final CloseableLock writeLock = lock.writeLock();
		private boolean closed;
		private final List<Limit> limits;
		private final List<ContainerListener> listeners;
		private Object userData;
		private boolean changed;
		private boolean wakeConsumers;

		/**
		 * Creates a new configuration updater.
		 */
		ConfigurationUpdater()
		{
			this.limits = new ArrayList<>(Bucket.this.limits);
			this.listeners = new ArrayList<>(Bucket.this.listeners);
			this.userData = Bucket.this.userData;
		}

		/**
		 * Returns the limits that the bucket must respect.
		 *
		 * @return the limits that the bucket must respect
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public List<Limit> limits()
		{
			ensureOpen();
			return limits;
		}

		/**
		 * Adds a limit that the bucket must respect.
		 *
		 * @param limitBuilder builds a Limit
		 * @return this
		 * @throws NullPointerException  if {@code limit} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater addLimit(Consumer<Limit.Builder> limitBuilder)
		{
			requireThat(limitBuilder, "limitBuilder").isNotNull();
			ensureOpen();
			// Adding a limit causes consumers to have to wait the same amount of time, or longer. No need to
			// wake sleeping consumers.
			limitBuilder.accept(new Limit.Builder(e ->
			{
				limits.add(e);
				changed = true;
			}));
			return this;
		}

		/**
		 * Removes a limit that the bucket must respect.
		 *
		 * @param limit a limit
		 * @return this
		 * @throws NullPointerException  if {@code limit} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater removeLimit(Limit limit)
		{
			requireThat(limit, "limit").isNotNull();
			ensureOpen();
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
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater clear()
		{
			ensureOpen();
			if (limits.isEmpty())
				return this;
			changed = true;
			limits.clear();
			return this;
		}

		/**
		 * Adds an event listener to the bucket.
		 *
		 * @param listener a listener
		 * @return this
		 * @throws NullPointerException  if {@code listener} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater addListener(ContainerListener listener)
		{
			requireThat(listener, "listener").isNotNull();
			ensureOpen();
			changed = true;
			listeners.add(listener);
			return this;
		}

		/**
		 * Removes an event listener from the bucket.
		 *
		 * @param listener a listener
		 * @return this
		 * @throws NullPointerException  if {@code listener} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater removeListener(ContainerListener listener)
		{
			requireThat(listener, "listener").isNotNull();
			ensureOpen();
			if (listeners.remove(listener))
				changed = true;
			return this;
		}

		/**
		 * Returns the user data associated with this bucket.
		 *
		 * @return the data associated with this bucket
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public Object userData()
		{
			ensureOpen();
			return userData;
		}

		/**
		 * Sets user data associated with this bucket.
		 *
		 * @param userData the data associated with this bucket
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
		 * Updates this Bucket's configuration.
		 *
		 * @throws IllegalArgumentException if {@code limits} is empty
		 */
		@Override
		public void close()
		{
			if (closed)
				return;
			closed = true;
			try
			{
				// There is no way to fix a ConfigurationUpdater once try-with-resources exits, so the updater is
				// closed and the write-lock released even if an exception is thrown.
				requireThat(limits, "limits").isNotEmpty();

				if (!changed)
					return;
				Bucket.this.limits = List.copyOf(limits);
				Bucket.this.listeners = List.copyOf(listeners);
				Bucket.this.userData = userData;
				if (wakeConsumers)
					tokensUpdated.signalAll();
			}
			finally
			{
				writeLock.close();
			}
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder(ConfigurationUpdater.class).
				add("limits", limits).
				add("userData", userData).
				add("changed", changed).
				toString();
		}
	}
}