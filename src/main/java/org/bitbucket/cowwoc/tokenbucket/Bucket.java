/*
 * Copyright 2018 Gili Tzabari.
 */
package org.bitbucket.cowwoc.tokenbucket;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.math.LongMath;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import static org.bitbucket.cowwoc.requirements.core.Requirements.requireThat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the <a href="https://en.wikipedia.org/wiki/Token_bucket">Token bucket algorithm</a>.
 * <p>
 * This class is thread-safe.
 *
 * @author Gili Tzabari
 */
public final class Bucket
{
	// Surprisingly, synchronized blocks are one of the fastest synchronization mechanisms as of JDK 9
	private final Object mutex = new Object();
	private final Set<Limit> limits = new HashSet<>();
	/**
	 * A map from each {@code Limit} to the time relative to which tokens were last refilled.
	 */
	private final Map<Limit, Instant> limitToRefillTime = new HashMap<>();
	/**
	 * A map from each {@code Limit} to the number of tokens available for consumption.
	 */
	private final Map<Limit, Long> limitToTokensAvailable = new HashMap<>();
	private final Logger log = LoggerFactory.getLogger(Bucket.class);

	/**
	 * Creates a new bucket that does not enforce any limits.
	 */
	public Bucket()
	{
	}

	/**
	 * Adds a limit that the bucket must respect.
	 *
	 * @param limit the limit
	 * @return true on success; false if the limit was already applied to the bucket
	 * @throws NullPointerException if {@code limit] is null
	 */
	public boolean addLimit(Limit limit)
	{
		synchronized (mutex)
		{
			boolean result = limits.add(limit);
			if (!result)
				return false;
			limitToRefillTime.put(limit, Instant.now());
			limitToTokensAvailable.put(limit, limit.getInitialTokens());
			// Adding a limit causes consumers to have to wait the same amount of time, or longer. No need to wake sleeping
			// consumers.
			return true;
		}
	}

	/**
	 * Removes a limit that the bucket must respect.
	 *
	 * @param limit the limit
	 * @return true on success; false if the limit could not be found
	 * @throws NullPointerException if {@code limit] is null
	 */
	public boolean removeLimit(Limit limit)
	{
		synchronized (mutex)
		{
			boolean result = limits.remove(limit);
			if (!result)
				return false;
			limitToRefillTime.remove(limit);
			limitToTokensAvailable.remove(limit);
			// Removing a limit might allow consumers to wait a shorter period of time. Wake all sleeping consumers and ask
			// them to recalculate their sleep time.
			mutex.notifyAll();
			return true;
		}
	}

	/**
	 * Replaces a limit while retaining the number of available tokens ({@code Limit.initialTokens} is ignored) If the
	 * number of available tokens is greater than the new limit's {@link Limit#maxTokens} it is reduced to
	 * {@code maxTokens}.
	 *
	 * @param oldLimit the old imit
	 * @param newLimit the new limit
	 * @return true if the old limit was found
	 * @throws NullPointerException if any of the arguments are null
	 */
	public boolean replaceLimit(Limit oldLimit, Limit newLimit)
	{
		requireThat("oldLimit", oldLimit).isNotNull();
		requireThat("newLimit", newLimit).isNotNull();
		synchronized (mutex)
		{
			boolean result = limits.contains(oldLimit);
			if (!result)
				return false;
			Instant refillTime = Instant.now();
			refillTokens(oldLimit, refillTime);

			limitToRefillTime.remove(oldLimit);
			Long tokensAvailable = limitToTokensAvailable.remove(oldLimit);
			limits.remove(oldLimit);
			assert (result): result;

			result = limits.add(newLimit);
			assert (result): result;

			limitToRefillTime.put(newLimit, refillTime);
			limitToTokensAvailable.put(newLimit, tokensAvailable);

			Function<Limit, Duration> periodPerToken = limit -> limit.getPeriod().dividedBy(limit.getTokensPerPeriod());
			Duration oldPeriodPerToken = periodPerToken.apply(oldLimit);
			Duration newPeriodPerToken = periodPerToken.apply(newLimit);

			if (newLimit.getMaxTokens() < oldLimit.getMaxTokens() || oldPeriodPerToken.compareTo(newPeriodPerToken) < 0)
			{
				// maxTokens has decreased, it might be impossible to consume the number of tokens that were requested.
				// period-per-token has decreased. Sleeping consumers need to wake up sooner.
				mutex.notifyAll();
			}
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
			limits.clear();
			limitToRefillTime.clear();
			limitToTokensAvailable.clear();
		}
	}

	/**
	 * @return the limits associated with this bucket
	 */
	public Set<Limit> getLimits()
	{
		return ImmutableSet.copyOf(limits);
	}

	/**
	 * Attempt to consume the specified number of tokens. Consumption is not guaranteed to be fair.
	 *
	 * @param tokens the number of tokens to acquire
	 * @return {@link Duration#ZERO} on success; otherwise, returns the minimum amount of time the caller would have to
	 *         wait until the requested number of tokens would become available
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero
	 */
	public Duration tryConsume(long tokens)
	{
		requireThat("tokens", tokens).isNotPositive();
		synchronized (mutex)
		{
			long tokensAvailable = limitToTokensAvailable.values().stream().min(Comparator.naturalOrder()).
				orElse(Long.MAX_VALUE);
			if (tokensAvailable >= tokens)
			{
				// No need to wait since we don't guarantee fairness
				for (Entry<Limit, Long> entry: limitToTokensAvailable.entrySet())
					entry.setValue(entry.getValue() - tokens);
				return Duration.ZERO;
			}
			return getDurationUntilTokensAvailable(tokens);
		}
	}

	/**
	 * Attempt to consume a single token.
	 *
	 * @return true on success
	 */
	public boolean tryConsume()
	{
		return tryConsume(1) == Duration.ZERO;
	}

	/**
	 * Blocks until consume the specified number of tokens. Consumption is not guaranteed to be fair.
	 *
	 * @param tokens the number of tokens to acquire
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become available
	 */
	public void consume(long tokens) throws InterruptedException
	{
		requireThat("tokens", tokens).isPositive();
		synchronized (mutex)
		{
			Duration durationToSleep = getDurationUntilTokensAvailable(tokens);
			if (durationToSleep.isZero())
			{
				// No need to wait since we don't guarantee fairness
				for (Entry<Limit, Long> entry: limitToTokensAvailable.entrySet())
				{
					assert (entry.getValue() >= tokens): "got: " + entry.getValue() + ", want: " + tokens;
					entry.setValue(entry.getValue() - tokens);
				}
				return;
			}
			do
			{
				mutex.wait(durationToSleep.getSeconds(), durationToSleep.getNano());
				refillTokens();
				durationToSleep = getDurationUntilTokensAvailable(tokens);
			}
			while (!durationToSleep.isZero());
			for (Entry<Limit, Long> entry: limitToTokensAvailable.entrySet())
			{
				entry.setValue(entry.getValue() - tokens);
				assert (entry.getValue() >= tokens): "got: " + entry.getValue() + ", want: " + tokens;
			}
		}
	}

	/**
	 * Blocks until consume the specified number of tokens. Consumption is not guaranteed to be fair.
	 *
	 * @param tokens  the number of tokens to acquire
	 * @param timeout the maximum amount of time to wait
	 * @param unit    the unit of {@code timeout}
	 * @return true if the tokens were consumed; false if a timeout occurred
	 * @throws IllegalArgumentException if {@code tokens} is negative or zero
	 * @throws InterruptedException     if the thread is interrupted while waiting for tokens to become available
	 */
	public boolean consume(long tokens, long timeout, TimeUnit unit) throws InterruptedException
	{
		requireThat("tokens", tokens).isPositive();
		Instant timeLimit = Instant.now().plus(timeout, chronoUnit(unit));
		synchronized (mutex)
		{
			Duration durationToSleep = getDurationUntilTokensAvailable(tokens);
			if (durationToSleep.isZero())
			{
				// No need to wait since we don't guarantee fairness
				for (Entry<Limit, Long> entry: limitToTokensAvailable.entrySet())
				{
					assert (entry.getValue() >= tokens): "got: " + entry.getValue() + ", want: " + tokens;
					entry.setValue(entry.getValue() - tokens);
				}
				return true;
			}
			do
			{
				if (!Instant.now().isBefore(timeLimit))
					return false;
				if (durationToSleep.isZero())
					break;
				log.info("consume() sleeping " + durationToSleep);
				mutex.wait(durationToSleep.toMillis(), durationToSleep.getNano() / 1000);
				refillTokens();
				durationToSleep = getDurationUntilTokensAvailable(tokens);
			}
			while (!durationToSleep.isZero());
			for (Entry<Limit, Long> entry: limitToTokensAvailable.entrySet())
			{
				assert (entry.getValue() >= tokens): "got: " + entry.getValue() + ", want: " + tokens;
				entry.setValue(entry.getValue() - tokens);
			}
			return true;
		}
	}

	/**
	 * Converts a {@code TimeUnit} to a {@code ChronoUnit}.
	 * <p>
	 * This handles the seven units declared in {@code TimeUnit}.
	 *
	 * @param unit the unit to convert, not null
	 * @return the converted unit, not null
	 */
	private static ChronoUnit chronoUnit(TimeUnit unit)
	{
		// Backport from https://bugs.openjdk.java.net/browse/JDK-8141452
		switch (unit)
		{
			case NANOSECONDS:
				return ChronoUnit.NANOS;
			case MICROSECONDS:
				return ChronoUnit.MICROS;
			case MILLISECONDS:
				return ChronoUnit.MILLIS;
			case SECONDS:
				return ChronoUnit.SECONDS;
			case MINUTES:
				return ChronoUnit.MINUTES;
			case HOURS:
				return ChronoUnit.HOURS;
			case DAYS:
				return ChronoUnit.DAYS;
			default:
				throw new IllegalArgumentException("Unknown TimeUnit constant");
		}
	}

	/**
	 * Consumes a single token.
	 *
	 * @throws InterruptedException if the thread is interrupted while waiting for tokens to become available
	 */
	public void consume() throws InterruptedException
	{
		consume(1);
	}

	/**
	 * Adds tokens that have been added since {@code refillTime}.
	 */
	private void refillTokens()
	{
		Instant now = Instant.now();
		for (Limit limit: limits)
			refillTokens(limit, now);
	}

	/**
	 * Adds tokens that have been added since {@code refillTime}.
	 *
	 * @param limit the limit to process
	 * @param now   the current time
	 */
	private void refillTokens(Limit limit, Instant now)
	{
		Instant refillTime = limitToRefillTime.get(limit);
		Duration elapsed = Duration.between(refillTime, now);
		if (elapsed.isNegative())
			return;
		// Fill tokens smoothly. For example, a refill rate of 60 tokens a minute will add 1 token per second as opposed
		// to 60 tokens all at once at the end of the minute.
		Duration periodPerToken = limit.getPeriod().dividedBy(limit.getTokensPerPeriod());
		long tokensToAdd = divide(elapsed, periodPerToken);
		Duration delta = periodPerToken.multipliedBy(tokensToAdd);
		limitToRefillTime.put(limit, refillTime.plus(delta));

		long tokensAvailable = limitToTokensAvailable.get(limit);
		limitToTokensAvailable.put(limit, Math.min(limit.getMaxTokens(),
			LongMath.saturatedAdd(tokensAvailable, tokensToAdd)));
	}

	/**
	 * @param first  the first duration
	 * @param second the second duration
	 * @return {@code first / second}
	 */
	private long divide(Duration first, Duration second)
	{
		assert (!first.isNegative()): "first was negative";
		assert (!second.isNegative()): "second was negative";
		assert (!second.isZero()): "second was zero";
		BigDecimal firstDecimal = getSeconds(first);
		BigDecimal secondDecimal = getSeconds(second);
		return firstDecimal.divideToIntegralValue(secondDecimal).longValueExact();
	}

	/**
	 * @param duration a duration
	 * @return the number of seconds in the duration
	 */
	private BigDecimal getSeconds(Duration duration)
	{
		return BigDecimal.valueOf(duration.getSeconds()).add(BigDecimal.valueOf(duration.getNano(), 9));
	}

	/**
	 * @param tokens the number of desired tokens
	 * @return the minimum amount of time until the requested number of tokens will be available
	 */
	private Duration getDurationUntilTokensAvailable(long tokens)
	{
		Duration result = Duration.ZERO;
		Ordering<Comparable<Duration>> ordering = Ordering.natural();
		for (Limit limit: limits)
			result = ordering.max(result, getDurationUntilTokensAvailable(tokens, limit));
		return result;
	}

	/**
	 * @param tokens the number of desired tokens
	 * @param limit  the limit
	 * @return the minimum amount of time until the requested number of tokens will be available
	 * @throws IllegalArgumentException if we'll never get enough tokens because one of the limits has a {@code maxTokens}
	 *                                  that is less than {@code tokens}
	 */
	private Duration getDurationUntilTokensAvailable(long tokens, Limit limit)
	{
		requireThat("limit.getMaxTokens()", limit.getMaxTokens()).isGreaterThanOrEqualTo("tokens", tokens);
		Long tokensAvailable = limitToTokensAvailable.get(limit);
		if (tokensAvailable > tokens)
			return Duration.ZERO;
		long tokensNeeded = tokens - tokensAvailable;
		long periodsToSleep = tokensNeeded / limit.getTokensPerPeriod();
		return limit.getPeriod().multipliedBy(periodsToSleep);
	}
}
