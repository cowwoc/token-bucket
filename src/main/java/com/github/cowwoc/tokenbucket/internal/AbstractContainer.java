package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.ConsumptionResult;
import com.github.cowwoc.tokenbucket.Container;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
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
			public long getTokensAvailable(AbstractContainer container)
			{
				return container.getTokensAvailable();
			}

			@Override
			public ConsumptionResult tryConsumeRange(AbstractContainer container, long minimumTokens,
			                                         long maximumTokens, Instant requestedAt)
			{
				try (CloseableLock ignored = container.lock.writeLock())
				{
					return container.consumptionPolicy.tryConsumeRange(minimumTokens, maximumTokens, requestedAt,
						container);
				}
			}
		};
	}

	/**
	 * Updates a child.
	 *
	 * @param child  the child
	 * @param update updates the child
	 * @throws NullPointerException if any of the arguments are null
	 */
	protected abstract void updateChild(Object child, Runnable update);

	/**
	 * Returns the number of tokens that are available, without triggering a refill.
	 *
	 * @return the number of tokens that are available
	 */
	protected abstract long getTokensAvailable();

	/**
	 * @return the logger associated with this bucket
	 */
	protected abstract Logger getLogger();

	/**
	 * Returns the lock over the bucket's state.
	 * <p>
	 * Locking policy:
	 * <p>
	 * <ul>
	 *   <li>public methods acquire locks.</li>
	 *   <li>non-public methods assume that the caller already acquired a lock.</li>
	 *   <li>{@link StampedLock} is better than {@link ReadWriteLock} in almost every way, but does not provide
	 *   {@link Condition} variables so did not use it: https://vimeo.com/74553130 and
	 * 	 https://www.javaspecialists.eu/talks/jfokus13/PhaserAndStampedLock.pdf</li>
	 * </ul>
	 */
	protected final ReadWriteLockAsResource lock;
	private final ConsumptionPolicy consumptionPolicy;
	/**
	 * A Condition that is signaled when the number of tokens has changed
	 */
	protected final Condition tokensUpdated;
	/**
	 * The parent container. {@code null} if there is no parent.
	 */
	protected Parent parent;
	protected Object userData;

	/**
	 * Creates a new AbstractBucket.
	 *
	 * @param lock              the lock over the bucket's state
	 * @param consumptionPolicy indicates how tokens are consumed
	 * @throws NullPointerException if any of the arguments are null
	 */
	protected AbstractContainer(ReadWriteLockAsResource lock, ConsumptionPolicy consumptionPolicy)
	{
		assertThat(lock, "lock").isNotNull();
		requireThat(consumptionPolicy, "consumptionPolicy").isNotNull();
		this.lock = lock;
		this.consumptionPolicy = consumptionPolicy;
		this.tokensUpdated = lock.newCondition();
	}

	@Override
	public Object getUserData()
	{
		return userData;
	}

	@Override
	public ConsumptionResult tryConsume()
	{
		Instant requestedAt = Instant.now();
		try (CloseableLock ignored = lock.writeLock())
		{
			return consumptionPolicy.tryConsumeRange(1, 1, requestedAt, this);
		}
	}

	@Override
	public ConsumptionResult tryConsume(long tokens)
	{
		requireThat(tokens, "tokens").isNotNegative();
		Instant requestedAt = Instant.now();
		try (CloseableLock ignored = lock.writeLock())
		{
			return consumptionPolicy.tryConsumeRange(tokens, tokens, requestedAt, this);
		}
	}

	@Override
	public ConsumptionResult tryConsumeRange(long minimumTokens, long maximumTokens)
	{
		requireThat(minimumTokens, "minimumTokens").isNotNegative();
		requireThat(maximumTokens, "maximumTokens").isNotNegative().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		Instant requestedAt = Instant.now();
		try (CloseableLock ignored = lock.writeLock())
		{
			return consumptionPolicy.tryConsumeRange(minimumTokens, maximumTokens, requestedAt, this);
		}
	}

	@Override
	public ConsumptionResult consume() throws InterruptedException
	{
		return consume(1);
	}

	@Override
	public ConsumptionResult consume(long tokens) throws InterruptedException
	{
		requireThat(tokens, "tokens").isNotNegative();
		Instant requestedAt = Instant.now();
		return consume(tokens, tokens, requestedAt, consumptionResult -> false);
	}

	@Override
	public ConsumptionResult consume(long tokens, long timeout, TimeUnit unit) throws InterruptedException
	{
		requireThat(tokens, "tokens").isNotNegative();
		requireThat(timeout, "timeout").isNotNegative();
		requireThat(unit, "unit").isNotNull();
		Instant requestedAt = Instant.now();
		Instant timeLimit = requestedAt.plus(timeout, unit.toChronoUnit());
		return consume(tokens, tokens, requestedAt,
			consumptionResult -> !consumptionResult.getTokensAvailableAt().isBefore(timeLimit));
	}

	@Override
	public ConsumptionResult consumeRange(long minimumTokens,
	                                      long maximumTokens) throws InterruptedException
	{
		requireThat(minimumTokens, "minimumTokens").isNotNegative();
		requireThat(maximumTokens, "maximumTokens").isNotNegative().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		Instant requestedAt = Instant.now();
		return consume(minimumTokens, maximumTokens, requestedAt, consumptionResult -> false);
	}

	@Override
	public ConsumptionResult consumeRange(long minimumTokens, long maximumTokens, long timeout,
	                                      TimeUnit unit)
		throws InterruptedException
	{
		requireThat(minimumTokens, "minimumTokens").isNotNegative();
		requireThat(maximumTokens, "maximumTokens").isNotNegative().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		requireThat(timeout, "timeout").isNotNegative();
		requireThat(unit, "unit").isNotNull();
		Instant requestedAt = Instant.now();
		Instant timeLimit = requestedAt.plus(timeout, unit.toChronoUnit());
		return consume(minimumTokens, maximumTokens, requestedAt,
			consumptionResult -> !consumptionResult.getTokensAvailableAt().isBefore(timeLimit));
	}

	/**
	 * Blocks until consume the specified number of tokens. Consumption is not guaranteed to be fair.
	 *
	 * @param minimumTokens the minimum number of tokens to consume (inclusive)
	 * @param maximumTokens the maximum number of tokens to consume (inclusive)
	 * @param requestedAt   the time at which the tokens were requested
	 * @param timeout       returns true if a timeout occurs
	 * @return the result of the operation
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code tokens} or {@code timeout} are negative or zero. If one of
	 *                                  the bucket limits has a {@code maxTokens} that is less than
	 *                                  {@code tokens}.
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become
	 *                                  available
	 */
	@CheckReturnValue
	private ConsumptionResult consume(long minimumTokens, long maximumTokens, Instant requestedAt,
	                                  Function<ConsumptionResult, Boolean> timeout)
		throws InterruptedException
	{
		assertThat(requestedAt, "requestedAt").isNotNull();
		Logger log = getLogger();
		while (true)
		{
			try (CloseableLock ignored = lock.writeLock())
			{
				ConsumptionResult consumptionResult = consumptionPolicy.tryConsumeRange(minimumTokens,
					maximumTokens, requestedAt, this);
				if (consumptionResult.isSuccessful() || timeout.apply(consumptionResult))
					return consumptionResult;
				requestedAt = Instant.now();
				Duration timeLeft = consumptionResult.getTokensAvailableIn();
				log.debug("Sleeping {}", timeLeft);
				Conditions.await(tokensUpdated, timeLeft);
			}
		}
	}
}