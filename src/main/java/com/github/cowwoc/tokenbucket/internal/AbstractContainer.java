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
				return container.lock.optimisticReadLock(container::getAvailableTokens);
			}

			@Override
			public List<Limit> getLimitsWithInsufficientTokens(AbstractContainer container, long tokens)
			{
				return container.lock.optimisticReadLock(() -> container.getLimitsWithInsufficientTokens(tokens));
			}

			@Override
			public long getMaximumTokens(AbstractContainer container)
			{
				return container.lock.optimisticReadLock(container::getMaximumTokens);
			}

			@Override
			public ConsumptionResult tryConsume(AbstractContainer container, long minimumTokens, long maximumTokens,
			                                    String nameOfMinimumTokens, Instant requestedAt, Instant consumedAt)
			{
				return container.consumptionFunction.tryConsume(minimumTokens, maximumTokens, nameOfMinimumTokens,
					requestedAt, consumedAt, container);
			}

			@Override
			public void setParent(AbstractContainer child, AbstractContainer parent)
			{
				requireThat(child, "child").isNotNull();
				if (parent != null)
				{
					AbstractContainer current = parent;
					do
					{
						if (current == child)
						{
							throw new IllegalArgumentException("\"child\" is already part of the hierarchy. Adding " +
								"\"parent\" would introduce a loop.");
						}
						AbstractContainer finalCurrent = current;
						current = current.lock.optimisticReadLock(() -> finalCurrent.parent);
					}
					while (current != null);
				}
				try (CloseableLock ignored = child.lock.writeLock())
				{
					child.parent = parent;
				}
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

	/**
	 * The parent container. {@code null} if there is no parent.
	 */
	protected AbstractContainer parent;
	protected List<ContainerListener> listeners;
	protected ConsumptionFunction consumptionFunction;
	protected Object userData;
	/**
	 * A lock over this object's state. See the {@link com.github.cowwoc.tokenbucket.internal locking policy}
	 * for more details.
	 */
	protected final ReentrantStampedLock lock = new ReentrantStampedLock();
	/**
	 * A lock used to signal/await {@code Condition} variables.
	 */
	protected final ReadWriteLockAsResource conditionLock = new ReadWriteLockAsResource();
	/**
	 * Notifies consumers that the number of tokens may have been changed as the result of a configuration
	 * update.
	 */
	protected final Condition tokensUpdated = conditionLock.newCondition();

	/**
	 * Creates a new AbstractContainer.
	 *
	 * @param listeners           the event listeners associated with this container
	 * @param userData            the data associated with this container
	 * @param consumptionFunction indicates how tokens are consumed
	 * @throws NullPointerException if {@code listeners}, {@code lock} or {@code consumptionFunction} are null
	 */
	protected AbstractContainer(List<ContainerListener> listeners, Object userData,
	                            ConsumptionFunction consumptionFunction)
	{
		if (assertionsAreEnabled())
		{
			requireThat(listeners, "listeners").isNotNull();
			requireThat(consumptionFunction, "consumptionFunction").isNotNull();
		}
		this.listeners = List.copyOf(listeners);
		this.userData = userData;
		this.consumptionFunction = consumptionFunction;
	}

	@Override
	public List<ContainerListener> getListeners()
	{
		return lock.optimisticReadLock(() -> listeners);
	}

	@Override
	public Object getUserData()
	{
		return lock.optimisticReadLock(() -> userData);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume()
	{
		Instant requestedAt = Instant.now();
		return consumptionFunction.tryConsume(1, 1, "tokensToConsume", requestedAt, requestedAt, this);
	}

	/**
	 * This method is only meant to be used by tests. It is equivalent to invoking {@link #tryConsume()} with
	 * the specified {@code requestedAt} value.
	 *
	 * @param requestedAt the time at which the tokens were requested
	 * @return the result of the operation
	 * @throws NullPointerException if {@code requestedAt} is null
	 */
	@CheckReturnValue
	protected ConsumptionResult tryConsume(Instant requestedAt)
	{
		return consumptionFunction.tryConsume(1, 1, "tokensToConsume", requestedAt, requestedAt, this);
	}

	@Override
	@CheckReturnValue
	public ConsumptionResult tryConsume(long tokens)
	{
		requireThat(tokens, "tokens").isPositive();
		Instant requestedAt = Instant.now();
		return consumptionFunction.tryConsume(tokens, tokens, "tokens", requestedAt, requestedAt, this);
	}

	/**
	 * This method is only meant to be used by tests. It is equivalent to invoking {@link #tryConsume()} with
	 * the specified {@code requestedAt} value.
	 *
	 * @param tokens      the number of tokens to consume
	 * @param requestedAt the time at which the tokens were requested
	 * @return the result of the operation
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero. If
	 *                                  {@code requestedAt > consumedAt}.
	 */
	@CheckReturnValue
	protected ConsumptionResult tryConsume(long tokens, Instant requestedAt)
	{
		requireThat(tokens, "tokens").isPositive();
		return consumptionFunction.tryConsume(tokens, tokens, "tokensToConsume", requestedAt, requestedAt, this);
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
		return consumptionFunction.tryConsume(minimumTokens, maximumTokens, "minimumTokens", requestedAt,
			requestedAt, this);
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
	 * @throws IllegalArgumentException if {@code minimumTokens > maximumTokens}. If one of the limits has a
	 *                                  {@link Limit#getMaximumTokens() maximumTokens} that is less than
	 *                                  {@code minimumTokens}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 * @implNote This method acquires its own locks
	 */
	@CheckReturnValue
	private ConsumptionResult consume(long minimumTokens, long maximumTokens, String nameOfMinimumTokens,
	                                  Instant requestedAt, Function<ConsumptionResult, Boolean> timeout)
		throws InterruptedException
	{
		assertThat(r ->
		{
			r.requireThat(nameOfMinimumTokens, "nameOfMinimumTokens").isNotEmpty();
			r.requireThat(requestedAt, "requestedAt").isNotNull();
		});
		Logger log = getLogger();
		while (true)
		{
			Instant consumedAt = Instant.now();
			ConsumptionResult consumptionResult = consumptionFunction.tryConsume(minimumTokens, maximumTokens,
				nameOfMinimumTokens, requestedAt, consumedAt, this);
			if (consumptionResult.isSuccessful() || timeout.apply(consumptionResult))
			{
				log.debug("consumptionResult: {}", consumptionResult);
				return consumptionResult;
			}
			log.debug("consumptionResult: {}", consumptionResult);
			Duration timeLeft = consumptionResult.getAvailableIn();
			log.debug("Sleeping {}. State before sleep: {}", timeLeft, this);
			beforeSleep(this, minimumTokens, requestedAt, consumptionResult.getAvailableAt(),
				consumptionResult.getBottlenecks());
			try (CloseableLock ignored = conditionLock.writeLock())
			{
				Conditions.await(tokensUpdated, timeLeft);
			}
			log.debug("State after sleep: {}", this);
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
		for (ContainerListener listener : lock.optimisticReadLock(() -> listeners))
			listener.beforeSleep(container, tokens, requestedAt, availableAt, bottleneck);
	}
}