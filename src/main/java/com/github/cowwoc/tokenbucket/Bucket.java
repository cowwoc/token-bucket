/*
 * Copyright 2018 Gili Tzabari.
 */
package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * An implementation of the <a href="https://en.wikipedia.org/wiki/Token_bucket">Token bucket algorithm</a>.
 * <p>
 * <b>Thread safety</b>: This class is thread-safe.
 */
public final class Bucket extends AbstractBucket
{
	private final Object mutex = new Object();
	private Set<Limit> limits = Set.of();
	private final Logger log = LoggerFactory.getLogger(Bucket.class);

	/**
	 * Creates a new bucket that does not enforce any limits.
	 */
	public Bucket()
	{
	}

	@Override
	protected Object getMutex()
	{
		return mutex;
	}

	@Override
	protected Logger getLogger()
	{
		return log;
	}

	/**
	 * Adds a limit that the bucket must respect.
	 * <ul>
	 * <li>{@code initialTokens} defaults to {@code 0}.</li>
	 * <li>{@code maxTokens} defaults to {@code Long.MAX_VALUE}.</li>
	 * <li>{@code minimumToRefill} defaults to {@code 1}.</li>
	 * </ul>
	 *
	 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
	 * @param period          indicates how often {@code tokensPerPeriod} should be added to the bucket
	 * @throws IllegalArgumentException if {@code tokensPerPeriod} or {@code period} are negative or zero
	 */
	@CheckReturnValue
	public LimitAdder addLimit(long tokensPerPeriod, Duration period)
	{
		return new LimitAdder(tokensPerPeriod, period);
	}

	/**
	 * Removes a limit that the bucket must respect.
	 *
	 * @param limit the limit
	 * @return true on success; false if the limit could not be found
	 * @throws NullPointerException if {@code limit} is null
	 */
	public boolean removeLimit(Limit limit)
	{
		requireThat(limit, "limit").isNotNull();
		synchronized (mutex)
		{
			Set<Limit> newLimits = new HashSet<>(limits);
			boolean result = newLimits.remove(limit);
			if (!result)
				return false;
			limits = Set.copyOf(newLimits);
			// Removing a limit might allow consumers to wait a shorter period of time. Wake all sleeping consumers and ask
			// them to recalculate their sleep time.
			mutex.notifyAll();
			return true;
		}
	}

	/**
	 * Removes all limits on this bucket.
	 */
	public void removeAllLimits()
	{
		synchronized (mutex)
		{
			limits = Set.of();
		}
	}

	/**
	 * Returns the limits associated with this bucket.
	 *
	 * @return an unmodifiable set
	 */
	public Set<Limit> getLimits()
	{
		return limits;
	}

	/**
	 * Sets the limits associated with bucket.
	 *
	 * @param limits the limits associated with bucket
	 * @throws NullPointerException if {@code limits} is null
	 */
	void setLimits(Set<Limit> limits)
	{
		assertThat(limits, "limits").isNotNull();
		this.limits = Set.copyOf(limits);
	}

	@Override
	protected ConsumptionResult tryConsumeRange(long minimumTokens, long maximumTokens,
	                                            Instant requestedAt)
	{
		requireThat(minimumTokens, "minimumTokens").isNotNegative();
		requireThat(maximumTokens, "maximumTokens").isNotNegative().
			isGreaterThanOrEqualTo(minimumTokens, "minimumTokens");
		assertThat(requestedAt, "requestedAt").isNotNull();
		synchronized (mutex)
		{
			for (Limit limit : limits)
				limit.refill(requestedAt);
			long tokensConsumed = 0;
			long tokensLeft = Long.MAX_VALUE;
			ConsumptionSimulation longestDelay = new ConsumptionSimulation(tokensConsumed, 0, requestedAt, requestedAt);
			Comparator<Duration> comparator = Comparator.naturalOrder();
			for (Limit limit : limits)
			{
				ConsumptionSimulation newConsumption = simulateConsumption(minimumTokens, maximumTokens, limit,
					requestedAt);
				tokensLeft = Math.min(tokensLeft, newConsumption.getTokensLeft());
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
				return new ConsumptionResult(this, minimumTokens, maximumTokens, tokensConsumed,
					tokensLeft, requestedAt, requestedAt);
			}
			assertThat(longestDelay.getTokensConsumed(), "longestDelay.getTokensConsumed()").isZero();
			return new ConsumptionResult(this, minimumTokens, maximumTokens, tokensConsumed, tokensLeft,
				requestedAt, longestDelay.getTokensAvailableAt());
		}
	}

	/**
	 * Simulates consumption with respect to a single limit.
	 *
	 * @param minimumTokensRequested the minimum number of tokens that were requested
	 * @param maximumTokensRequested the maximum number of tokens that were requested
	 * @param limit                  the limit
	 * @param requestedAt            the time at which the user attempted to consume the tokens
	 * @return the simulated consumption
	 * @throws IllegalArgumentException if the limit has a {@code maxTokens} that is less than
	 *                                  {@code minimumTokensRequested}
	 */
	private ConsumptionSimulation simulateConsumption(long minimumTokensRequested, long maximumTokensRequested,
	                                                  Limit limit, Instant requestedAt)
	{
		requireThat(limit.getMaxTokens(), "limit.getMaxTokens()").
			isGreaterThanOrEqualTo(minimumTokensRequested, "minimumTokensRequested");
		long tokensAvailable = limit.getTokensAvailable();
		Instant tokensAvailableAt;
		long tokensConsumed;
		if (tokensAvailable < minimumTokensRequested)
		{
			long tokensNeeded = minimumTokensRequested - tokensAvailable;
			long periodsToSleep = tokensNeeded / limit.getTokensPerPeriod();
			Instant lastRefillTime = limit.getLastRefilledAt();
			Duration period = limit.getPeriod();
			tokensAvailableAt = lastRefillTime.plus(period.multipliedBy(periodsToSleep));
			tokensConsumed = 0;
		}
		else
		{
			tokensAvailableAt = requestedAt;
			tokensConsumed = Math.min(maximumTokensRequested, tokensAvailable);
			tokensAvailable -= tokensConsumed;
		}
		return new ConsumptionSimulation(tokensConsumed, tokensAvailable, requestedAt, tokensAvailableAt);
	}

	@Override
	public String toString()
	{
		return "limits: " + limits;
	}

	/**
	 * A rate limit.
	 */
	public final class LimitAdder
	{
		private final long tokensPerPeriod;
		private final Duration period;
		private long initialTokens;
		private long maxTokens;
		private long minimumToRefill;

		/**
		 * Adds a limit that the bucket must respect.
		 * <ul>
		 * <li>{@code initialTokens} defaults to {@code 0}.</li>
		 * <li>{@code maxTokens} defaults to {@code Long.MAX_VALUE}.</li>
		 * <li>{@code minimumToRefill} defaults to {@code 1}.</li>
		 * </ul>
		 *
		 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
		 * @param period          indicates how often {@code tokensPerPeriod} should be added to the bucket
		 * @throws IllegalArgumentException if {@code tokensPerPeriod} or {@code period} are negative or zero
		 */
		private LimitAdder(long tokensPerPeriod, Duration period)
		{
			requireThat(tokensPerPeriod, "tokensPerPeriod").isPositive();
			requireThat(period, "period").isGreaterThan(Duration.ZERO);
			this.tokensPerPeriod = tokensPerPeriod;
			this.period = period;
			this.initialTokens = 0;
			this.maxTokens = Long.MAX_VALUE;
			this.minimumToRefill = 1;
		}

		/**
		 * Sets the initial amount of tokens in the bucket.
		 *
		 * @param initialTokens the initial amount of tokens in the bucket
		 * @return this
		 * @throws IllegalArgumentException if {@code initialTokens} is negative
		 */
		@CheckReturnValue
		public LimitAdder initialTokens(long initialTokens)
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
		public LimitAdder maxTokens(long maxTokens)
		{
			requireThat(maxTokens, "maxTokens").isPositive();
			this.maxTokens = maxTokens;
			return this;
		}

		/**
		 * Sets the minimum number of tokens to add to the limit.
		 *
		 * @param minimumToRefill the minimum number of tokens to add to the limit
		 * @return this
		 * @throws IllegalArgumentException if {@code minimumToRefill} is negative
		 */
		@CheckReturnValue
		public LimitAdder minimumToRefill(long minimumToRefill)
		{
			requireThat(minimumToRefill, "minimumToRefill").isPositive();
			this.minimumToRefill = minimumToRefill;
			return this;
		}

		/**
		 * Adds a new limit to the bucket with this configuration. If an existing limit already has this
		 * configuration, it is returned instead.
		 *
		 * @return the new or existing Limit
		 */
		public Limit apply()
		{
			synchronized (mutex)
			{
				for (Limit limit : limits)
				{
					if (limit.getTokensPerPeriod() == tokensPerPeriod &&
						limit.getPeriod().equals(period) &&
						limit.getInitialTokens() == initialTokens &&
						limit.getMaxTokens() == maxTokens &&
						limit.getMinimumToRefill() == minimumToRefill)
						return limit;
				}
				Limit limit = new Limit(tokensPerPeriod, period, initialTokens, maxTokens, minimumToRefill);
				Set<Limit> newLimits = new HashSet<>(limits);
				newLimits.add(limit);
				limits = Set.copyOf(newLimits);
				// Adding a limit causes consumers to have to wait the same amount of time, or longer. No need to
				// wake sleeping consumers.
				return limit;
			}
		}
	}

	/**
	 * The result of a simulated token consumption.
	 */
	private static final class ConsumptionSimulation
	{
		private final long tokensConsumed;
		private final long tokensLeft;
		private final Instant requestedAt;
		private final Instant tokensAvailableAt;

		/**
		 * Creates the result of a simulated token consumption.
		 *
		 * @param tokensConsumed    the number of tokens that were consumed
		 * @param tokensLeft        the number of tokens that were left for consumption
		 * @param requestedAt       the time at which the user requested to consume tokens
		 * @param tokensAvailableAt the time at which the requested tokens will become available
		 * @throws NullPointerException     if any of the arguments are null
		 * @throws IllegalArgumentException if any of the arguments are negative. If
		 *                                  {@code minimumTokensRequested > maximumTokensRequested}. If
		 *                                  {@code tokensConsumed > 0 && tokensConsumed < minimumTokensRequested}
		 */
		public ConsumptionSimulation(long tokensConsumed, long tokensLeft, Instant requestedAt,
		                             Instant tokensAvailableAt)
		{
			assertThat(tokensConsumed, "tokensConsumed").isNotNegative();
			assertThat(tokensLeft, "tokensLeft").isNotNegative();
			assertThat(requestedAt, "requestedAt").isNotNull();
			assertThat(tokensAvailableAt, "tokensAvailableAt").isNotNull();
			this.tokensConsumed = tokensConsumed;
			this.tokensLeft = tokensLeft;
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
		 * Returns the number of tokens that were left for consumption after processing the request.
		 *
		 * @return the number of tokens that were left for consumption after processing the request
		 */
		public long getTokensLeft()
		{
			return tokensLeft;
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
				tokensLeft + ", requestedAt: " + requestedAt + ", tokensAvailableAt: " + tokensAvailableAt;
		}
	}
}