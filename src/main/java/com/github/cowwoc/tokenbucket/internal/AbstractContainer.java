package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.tokenbucket.ConsumptionResult;
import com.github.cowwoc.tokenbucket.Container;
import com.github.cowwoc.tokenbucket.ContainerListener;
import com.github.cowwoc.tokenbucket.Limit;
import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.assertionsAreEnabled;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * Functionality common to all container classes.
 */
public abstract class AbstractContainer implements Container
{
	static
	{
		SharedSecrets.INSTANCE.containerSecrets = new ContainerSecrets()
		{
			@Override
			public long getAvailableTokens(AbstractContainer container)
			{
				return container.getAvailableTokens();
			}

			@Override
			public List<Limit> getLimitsWithInsufficientTokens(AbstractContainer container, long tokens)
			{
				return container.getLimitsWithInsufficientTokens(tokens);
			}

			@Override
			public long getMaximumTokens(AbstractContainer container)
			{
				return container.getMaximumTokens();
			}

			@Override
			public ConsumptionResult tryConsume(AbstractContainer container, long minimumTokens, long maximumTokens,
			                                    String nameOfMinimumTokens, Instant requestedAt, Instant consumedAt)
			{
				return container.consumptionFunction.tryConsume(minimumTokens, maximumTokens, nameOfMinimumTokens,
					requestedAt, consumedAt, container);
			}
		};
	}

	/**
	 * Returns the number of tokens that are available, without triggering a refill.
	 *
	 * @return the number of tokens that are available
	 */
	protected abstract long getAvailableTokens();

	/**
	 * Returns the maximum number of tokens that this container can ever hold.
	 *
	 * @return the maximum number of tokens that this container can ever hold
	 */
	protected abstract long getMaximumTokens();

	/**
	 * Returns the list of limits with less than the specified number of tokens.
	 *
	 * @param tokens the minimum number of tokens to consume
	 * @return the list of limits with less than the specified number of tokens
	 */
	protected abstract List<Limit> getLimitsWithInsufficientTokens(long tokens);

	/**
	 * @return the logger associated with this bucket
	 */
	protected abstract Logger getLogger();

	protected List<ContainerListener> listeners;
	/**
	 * Returns the lock over the bucket's state.
	 * <p>
	 * Locking policy:
	 * <p>
	 * <ul>
	 *   <li>public methods acquire locks.</li>
	 *   <li>non-public methods assume that the caller already acquired a lock.</li>
	 *   <li>{@link StampedLock} is better than {@link ReadWriteLock} in almost every way, but does not provide
	 *   {@link Condition} variables so we did not use it. See the following
	 *   <a href="https://vimeo.com/74553130">video</a> and
	 * 	 <a href="https://www.javaspecialists.eu/talks/jfokus13/PhaserAndStampedLock.pdf">article</a></li>
	 * </ul>
	 */
	protected final ReadWriteLockAsResource lock;
	protected ConsumptionFunction consumptionFunction;
	/**
	 * A Condition that is signaled when the number of tokens has changed
	 */
	protected final Condition tokensUpdated;
	/**
	 * The parent container. {@code null} if there is no parent.
	 */
	protected AbstractContainer parent;
	protected Object userData;
	protected boolean userDataInToString;

	/**
	 * Creates a new AbstractBucket.
	 *
	 * @param listeners           the event listeners associated with this container
	 * @param userData            the data associated with this container
	 * @param userDataInToString  true if the value of {@code userData} should be in {@link #toString()}
	 * @param consumptionFunction indicates how tokens are consumed
	 * @param lock                the lock over the bucket's state
	 * @throws NullPointerException if {@code listeners}, {@code lock} or {@code consumptionFunction} are null
	 */
	protected AbstractContainer(List<ContainerListener> listeners, Object userData, boolean userDataInToString,
	                            ConsumptionFunction consumptionFunction, ReadWriteLockAsResource lock)
	{
		if (assertionsAreEnabled())
		{
			requireThat(listeners, "listeners").isNotNull();
			requireThat(consumptionFunction, "consumptionFunction").isNotNull();
			requireThat(lock, "lock").isNotNull();
		}
		this.listeners = List.copyOf(listeners);
		this.userData = userData;
		this.userDataInToString = userDataInToString;
		this.lock = lock;
		this.consumptionFunction = consumptionFunction;
		this.tokensUpdated = lock.newCondition();
	}

	/**
	 * Returns the parent container.
	 *
	 * @return null if this is the highest-level container
	 */
	public Container getParent()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return parent;
		}
	}

	@Override
	public List<ContainerListener> getListeners()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return listeners;
		}
	}

	@Override
	public Object getUserData()
	{
		return userData;
	}

	@Override
	public boolean isUserDataInToString()
	{
		return userDataInToString;
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume()
	{
		Instant requestedAt = Instant.now();
		try (CloseableLock ignored = lock.writeLock())
		{
			Instant consumedAt = Instant.now();
			return consumptionFunction.tryConsume(1, 1, "tokensToConsume", requestedAt, consumedAt, this);
		}
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens)
	{
		requireThat(tokens, "tokens").isPositive();
		Instant requestedAt = Instant.now();
		try (CloseableLock ignored = lock.writeLock())
		{
			Instant consumedAt = Instant.now();
			return consumptionFunction.tryConsume(tokens, tokens, "tokens", requestedAt, consumedAt, this);
		}
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens, long timeout, TimeUnit unit) throws InterruptedException
	{
		requireThat(tokens, "tokens").isPositive();
		requireThat(timeout, "timeout").isNotNegative();
		requireThat(unit, "unit").isNotNull();
		Instant requestedAt = Instant.now();
		Instant timeLimit = requestedAt.plus(timeout, unit.toChronoUnit());
		return consume(tokens, tokens, "tokens", requestedAt,
			consumptionResult -> !consumptionResult.getAvailableAt().isBefore(timeLimit));
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long minimumTokens, long maximumTokens)
	{
		requireThat(minimumTokens, "minimumTokens").isPositive();
		requireThat(maximumTokens, "maximumTokens").isPositive().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		Instant requestedAt = Instant.now();
		try (CloseableLock ignored = lock.writeLock())
		{
			Instant consumedAt = Instant.now();
			return consumptionFunction.tryConsume(minimumTokens, maximumTokens, "minimumTokens", requestedAt,
				consumedAt, this);
		}
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long minimumTokens, long maximumTokens, long timeout, TimeUnit unit)
		throws InterruptedException
	{
		requireThat(minimumTokens, "minimumTokens").isPositive();
		requireThat(maximumTokens, "maximumTokens").isPositive().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		requireThat(timeout, "timeout").isNotNegative();
		requireThat(unit, "unit").isNotNull();
		Instant requestedAt = Instant.now();
		Instant timeLimit = requestedAt.plus(timeout, unit.toChronoUnit());
		return consume(minimumTokens, maximumTokens, "minimumTokens", requestedAt,
			consumptionResult -> !consumptionResult.getAvailableAt().isBefore(timeLimit));
	}

	@Override
	public ConsumptionResult consume() throws InterruptedException
	{
		return consume(1);
	}

	@Override
	public ConsumptionResult consume(long tokens) throws InterruptedException
	{
		requireThat(tokens, "tokens").isPositive();
		Instant requestedAt = Instant.now();
		return consume(tokens, tokens, "tokens", requestedAt, consumptionResult -> false);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult consume(long minimumTokens, long maximumTokens) throws InterruptedException
	{
		requireThat(minimumTokens, "minimumTokens").isPositive();
		requireThat(maximumTokens, "maximumTokens").isPositive().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		Instant requestedAt = Instant.now();
		return consume(minimumTokens, maximumTokens, "minimumTokens", requestedAt, consumptionResult -> false);
	}

	/**
	 * Consumes {@code [minimumTokens, maximumTokens]} tokens, blocking until they become available.
	 * Consumption order is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @param requestedAt   the time at which the tokens were requested
	 * @param timeout       returns true if a timeout occurs
	 * @return the result of the operation
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code nameOfMinimumTokens} is empty. If {@code tokens} or
	 *                                  {@code timeout} are negative or zero. If one of the bucket limits has
	 *                                  a {@code maximumTokens} that is less than {@code tokens}. If
	 *                                  {@code consumedAt < requestedAt}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	private ConsumptionResult consume(long minimumTokens, long maximumTokens, String nameOfMinimumTokens,
	                                  Instant requestedAt, Function<ConsumptionResult, Boolean> timeout)
		throws InterruptedException
	{
		assertThat(nameOfMinimumTokens, "nameOfMinimumTokens").isNotEmpty();
		assertThat(requestedAt, "requestedAt").isNotNull();
		Logger log = getLogger();
		try (CloseableLock ignored = lock.writeLock())
		{
			while (true)
			{
				Instant consumedAt = Instant.now();
				assertThat(consumedAt, "consumedAt").isGreaterThanOrEqualTo(requestedAt, "requestedAt");
				ConsumptionResult consumptionResult = consumptionFunction.tryConsume(minimumTokens, maximumTokens,
					nameOfMinimumTokens, requestedAt, consumedAt, this);
				if (consumptionResult.isSuccessful() || timeout.apply(consumptionResult))
				{
					log.debug("consumptionResult: {}", consumptionResult);
					return consumptionResult;
				}
				log.debug("consumptionResult: {}", consumptionResult);
				Duration timeLeft = consumptionResult.getAvailableIn();
				log.debug("Sleeping {}", timeLeft);
				log.debug("State before sleep: {}", this);
				beforeSleep(this, minimumTokens, requestedAt, consumptionResult.getAvailableAt(),
					consumptionResult.getBottlenecks());
				Conditions.await(tokensUpdated, timeLeft);
				log.debug("State after sleep: {}", this);
			}
		}
	}

	/**
	 * Invoked before sleeping to wait for more tokens.
	 *
	 * @param container   the container the thread is waiting on
	 * @param tokens      the number of tokens that the thread is waiting for
	 * @param requestedAt the time at which the tokens were requested
	 * @param availableAt the time at which the requested tokens are expected to become available
	 * @param bottleneck  the list of Limits that are preventing tokens from being consumed
	 * @throws InterruptedException if the operation should be interrupted
	 */
	protected void beforeSleep(Container container, long tokens, Instant requestedAt, Instant availableAt,
	                           List<Limit> bottleneck) throws InterruptedException
	{
		if (parent != null)
			parent.beforeSleep(container, tokens, requestedAt, availableAt, bottleneck);
		for (ContainerListener listener : listeners)
			listener.beforeSleep(container, tokens, requestedAt, availableAt, bottleneck);
	}
}