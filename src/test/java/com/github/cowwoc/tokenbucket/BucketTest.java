package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.Limit.Builder;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class BucketTest
{
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void requireAtLeastOneLimit() throws IllegalArgumentException
	{
		Bucket.builder().build();
	}

	@Test
	public void bucketIsUnique()
	{
		Bucket first = Bucket.builder().addLimit(Builder::build).build();
		Bucket second = Bucket.builder().addLimit(Builder::build).build();
		requireThat(first, "first").isNotEqualTo(second, "second");
		requireThat(first.hashCode(), "first.hashCode()").isNotEqualTo(second.hashCode(), "second.hashCode()");
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void requireAtLeastOneBucket()
	{
		ContainerList.builder().
			consumeFromOne(SelectionPolicy.roundRobin()).
			build();
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void consumeMoreThanLimitMinimum()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit -> limit.maximumTokens(10).build()).
			build();
		//noinspection ResultOfMethodCallIgnored
		bucket.tryConsume(11, 20);
	}

	@Test
	public void roundingError()
	{
		int tokens = 9;
		int seconds = 10;
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.tokensPerPeriod(tokens).
					period(Duration.ofSeconds(seconds)).
					minimumRefill(1).
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		limit.lastRefilledAt = Instant.now();
		Instant requestAt = limit.lastRefilledAt;
		Duration timeIncrement = limit.getPeriod().dividedBy(seconds);
		for (int i = 1; i <= seconds; ++i)
		{
			requestAt = requestAt.plus(timeIncrement);
			limit.refill(requestAt);
		}
		requireThat(limit.getAvailableTokens(), "limit.getAvailableTokens()").
			isEqualTo(limit.getTokensPerPeriod());
	}

	@Test
	public void negativeInitialTokens() throws IllegalArgumentException
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(-100).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					minimumRefill(1).
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		requireThat(limit.getAvailableTokens(), "limit.getAvailableTokens()").isNegative();

		ConsumptionResult consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isGreaterThanOrEqualTo(Duration.ofSeconds(90));
	}

	@Test
	public void negativeAvailableTokens() throws IllegalArgumentException
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(1).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					minimumRefill(1).
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		requireThat(limit.getAvailableTokens(), "limit.getAvailableTokens()").isPositive();
		ConsumptionResult consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isEqualTo(Duration.ZERO);

		limit.updateConfiguration().availableTokens(-100).apply();
		requireThat(limit.getAvailableTokens(), "limit.getAvailableTokens()").isNegative();
		consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isGreaterThanOrEqualTo(Duration.ofSeconds(90));
	}

	@Test
	public void consumeMinimumAvailableTokens() throws IllegalArgumentException
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(10).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					minimumRefill(1).
					build()).
			addLimit(limit ->
				limit.initialTokens(100).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					minimumRefill(1).
					build()).
			build();

		ConsumptionResult consumptionResult = bucket.tryConsume(1, 1000);
		requireThat(consumptionResult.getTokensConsumed(), "consumptionResult.getTokensConsumed()").
			isEqualTo(10L);
	}
}