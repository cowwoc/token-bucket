package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.Limit.ConsumptionSimulation;
import com.github.cowwoc.tokenbucket.internal.AbstractContainer;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.ReadWriteLockAsResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
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
	private List<Limit> limits;

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
	 * @param lock      the lock over the bucket's state
	 * @param listeners the event listeners associated with this bucket
	 * @param userData  the data associated with this bucket
	 * @throws NullPointerException     if {@code limits}, {@code listeners} or {@code lock} are null
	 * @throws IllegalArgumentException if {@code limits} is empty
	 */
	private Bucket(List<Limit> limits, List<ContainerListener> listeners, Object userData,
	               ReadWriteLockAsResource lock)
	{
		super(listeners, userData, lock, Bucket::tryConsume);
		assertThat(limits, "limits").isNotEmpty();
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
	 * Attempts to consume {@code [minimumTokens, maximumTokens]}. Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokens       the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens       the maximum  number of tokens to consume (inclusive)
	 * @param nameOfMinimumTokens the name of the {@code minimumTokens} parameter
	 * @param requestedAt         the time at which the tokens were requested
	 * @param abstractBucket      the container
	 * @return the minimum amount of time until the requested number of tokens will be available
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code nameOfMinimumTokens} is empty. If
	 *                                  {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@code maximumTokens} that is less than {@code minimumTokens}.
	 */
	private static ConsumptionResult tryConsume(long minimumTokens, long maximumTokens,
	                                            String nameOfMinimumTokens, Instant requestedAt,
	                                            AbstractContainer abstractBucket)
	{
		assertThat(nameOfMinimumTokens, "nameOfMinimumTokens").isNotEmpty();
		assertThat(requestedAt, "requestedAt").isNotNull();
		Bucket bucket = (Bucket) abstractBucket;
		List<Limit> limits = bucket.getLimits();
		for (Limit limit : limits)
		{
			requireThat(minimumTokens, nameOfMinimumTokens).
				isLessThanOrEqualTo(limit.getMaximumTokens(), "limit.getMaximumTokens()");
			limit.refill(requestedAt);
		}
		long tokensConsumed = Long.MAX_VALUE;
		ConsumptionSimulation longestDelay = new ConsumptionSimulation(tokensConsumed, requestedAt, requestedAt);
		Comparator<Duration> comparator = Comparator.naturalOrder();
		Limit bottleneck = null;
		for (Limit limit : limits)
		{
			ConsumptionSimulation newConsumption = limit.simulateConsumption(minimumTokens, maximumTokens, requestedAt);
			tokensConsumed = Math.min(tokensConsumed, newConsumption.getTokensConsumed());
			if (comparator.compare(newConsumption.getAvailableIn(), longestDelay.getAvailableIn()) > 0)
			{
				longestDelay = newConsumption;
				bottleneck = limit;
			}
		}
		if (tokensConsumed > 0)
		{
			// If there are any remaining tokens after consumption then wake up other consumers
			long minimumAvailableTokens = Long.MAX_VALUE;
			for (Limit limit : limits)
			{
				long availableTokens = limit.consume(tokensConsumed);
				if (availableTokens < minimumAvailableTokens)
					minimumAvailableTokens = availableTokens;
			}
			if (minimumAvailableTokens > 0)
				bucket.tokensUpdated.signalAll();
			return new ConsumptionResult(bucket, minimumTokens, maximumTokens, tokensConsumed, requestedAt,
				requestedAt, List.of());
		}
		assertThat(longestDelay.getTokensConsumed(), "longestDelay.getTokensConsumed()").isZero();
		assert (bottleneck != null);
		return new ConsumptionResult(bucket, minimumTokens, maximumTokens, tokensConsumed, requestedAt,
			longestDelay.getAvailableAt(), List.of(bottleneck));
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

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens)
	{
		return super.tryConsume(tokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long minimumTokens, long maximumTokens)
	{
		return super.tryConsume(minimumTokens, maximumTokens);
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
	public ConsumptionResult consume(long minimumTokens, long maximumTokens) throws InterruptedException
	{
		return super.consume(minimumTokens, maximumTokens);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consume(long minimumTokens, long maximumTokens, long timeout, TimeUnit unit)
		throws InterruptedException
	{
		return super.consume(minimumTokens, maximumTokens, timeout, unit);
	}

	@Override
	public String toString()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			StringJoiner properties = new StringJoiner(",\n");

			StringJoiner limitsJoiner = new StringJoiner(", ");
			for (Limit limit : limits)
				limitsJoiner.add(limit.toString());
			properties.add("limits: " + limitsJoiner);

			properties.add("userData: " + userData);
			return "\n" +
				"[\n" +
				"\t" + properties.toString().replaceAll("\n", "\n\t") + "\n" +
				"]";
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
			requireThat(limits, "limits").isNotEmpty();
			if (closed)
				return;
			closed = true;
			try
			{
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
	}
}