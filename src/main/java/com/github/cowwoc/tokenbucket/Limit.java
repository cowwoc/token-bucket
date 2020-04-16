/*
 * Copyright 2018 Gili Tzabari.
 */
package com.github.cowwoc.tokenbucket;

import java.time.Duration;
import java.util.Objects;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A rate limit.
 *
 * @author Gili Tzabari
 */
public final class Limit
{
	private final long tokensPerPeriod;
	private final Duration period;
	private final long initialTokens;
	private final long maxTokens;

	/**
	 * Creates a new limit.
	 *
	 * @param tokensPerPeriod the amount of tokens to add to the bucket every {@code period}
	 * @param period          indicates how often {@code tokensPerPeriod} should be added to the bucket
	 * @param initialTokens   the initial amount of tokens in the bucket
	 * @param maxTokens       the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens
	 *                        are discarded)
	 * @throws NullPointerException     if {@code period} is null
	 * @throws IllegalArgumentException if any of the arguments are not negative or zero; if
	 *                                  {@code initialTokens > maxTokens} or {@code tokensPerPeriod > maxTokens}
	 */
	public Limit(long tokensPerPeriod, Duration period, long initialTokens, long maxTokens)
	{
		requireThat(tokensPerPeriod, "tokensPerPeriod").isPositive();
		requireThat(period, "period").isNotNull().isGreaterThan(Duration.ZERO);
		requireThat(initialTokens, "initialTokens").isNotNegative();
		requireThat(maxTokens, "maxTokens").isPositive().isGreaterThan(tokensPerPeriod, "tokensPerPeriod").
			isGreaterThan(initialTokens, "initialTokens");
		this.tokensPerPeriod = tokensPerPeriod;
		this.period = period;
		this.initialTokens = initialTokens;
		this.maxTokens = maxTokens;
	}

	/**
	 * @return the amount of tokens to add to the bucket every {@code period}
	 */
	public long getTokensPerPeriod()
	{
		return tokensPerPeriod;
	}

	/**
	 * @return indicates how often {@code tokensPerPeriod} should be added to the bucket
	 */
	public Duration getPeriod()
	{
		return period;
	}

	/**
	 * @return the initial amount of tokens that the bucket starts with
	 */
	public long getInitialTokens()
	{
		return initialTokens;
	}

	/**
	 * @return the maximum amount of tokens that the bucket may hold before overflowing (subsequent tokens are discarded)
	 */
	public long getMaxTokens()
	{
		return maxTokens;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(tokensPerPeriod, period, initialTokens, maxTokens);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof Limit))
			return false;
		Limit other = (Limit) o;
		return tokensPerPeriod == other.tokensPerPeriod && initialTokens == other.initialTokens &&
			maxTokens == other.maxTokens && period.equals(other.period);
	}

	@Override
	public String toString()
	{
		return "tokensPerPeriod: " + tokensPerPeriod + ", period: " + period + ", initialTokens: " +
			initialTokens + ", maxTokens: " + maxTokens;
	}
}
