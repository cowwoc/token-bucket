package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.Limit.Builder;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class BucketTest
{
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void requireAtLeastOneLimit()
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
					refillSize(1).
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
			requireThat(limit.availableTokens, "limit.availableTokens").
				isEqualTo((long) ((double) tokens * i / seconds));
		}
		requireThat(limit.availableTokens, "limit.availableTokens").
			isEqualTo(limit.getTokensPerPeriod());

		for (int i = 1; i <= seconds; ++i)
		{
			requestAt = requestAt.plus(timeIncrement);
			limit.refill(requestAt);
			requireThat(limit.availableTokens, "limit.availableTokens").
				isEqualTo(tokens + (long) ((double) tokens * i / seconds));
		}
		requireThat(limit.availableTokens, "limit.availableTokens").
			isEqualTo(2 * limit.getTokensPerPeriod());
	}

	@Test
	public void negativeInitialTokens()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(-100).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					refillSize(1).
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		requireThat(limit.availableTokens, "limit.getAvailableTokens()").isNegative();

		ConsumptionResult consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isGreaterThanOrEqualTo(Duration.ofSeconds(90));
	}

	@Test
	public void negativeAvailableTokens()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(1).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					refillSize(1).
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		requireThat(limit.availableTokens, "limit.getAvailableTokens()").isPositive();
		ConsumptionResult consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isEqualTo(Duration.ZERO);

		limit.updateConfiguration().availableTokens(-100).apply();
		requireThat(limit.availableTokens, "limit.getAvailableTokens()").isNegative();
		consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isGreaterThanOrEqualTo(Duration.ofSeconds(90));
	}

	@Test
	public void consumeMinimumAvailableTokens()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(10).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					refillSize(1).
					build()).
			addLimit(limit ->
				limit.initialTokens(100).
					tokensPerPeriod(1).
					period(Duration.ofSeconds(1)).
					refillSize(1).
					build()).
			build();

		ConsumptionResult consumptionResult = bucket.tryConsume(1, 1000);
		requireThat(consumptionResult.getTokensConsumed(), "consumptionResult.getTokensConsumed()").
			isEqualTo(10L);
	}

	@Test
	public void sleepPartialPeriod()
	{
		// When tokensNeeded < tokensPerPeriod, simulateConsumption() incorrectly rounded periodsToSleep down
		// to zero. This, in turn, caused an assertion error to get thrown.
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(0).
					period(Duration.ofMinutes(10)).
					tokensPerPeriod(6000).
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		requireThat(limit.availableTokens, "limit.getAvailableTokens()").isZero();

		ConsumptionResult consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getTokensConsumed(), "consumptionResult.getTokensConsumed()").
			isEqualTo(0L);
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isBetween(Duration.ZERO, Duration.ofMinutes(10));
	}

	@Test
	public void refillPartialPeriod()
	{
		// Make sure that we refill tokens correctly mid-period
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(0).
					period(Duration.ofSeconds(10)).
					tokensPerPeriod(10).
					refillSize(2).
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		long tokensBefore = limit.availableTokens;
		limit.refill(limit.lastRefilledAt.plusSeconds(5));
		long tokensAfter = limit.availableTokens;
		long tokensAdded = tokensAfter - tokensBefore;
		requireThat(tokensAdded, "tokensAdded").isEqualTo(4L);
	}

	@Test
	public void bottleneckListIsMinimal()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.tokensPerPeriod(1).
					period(Duration.ofMinutes(1)).
					refillSize(1).
					userData("first").
					build()).
			addLimit(limit ->
				limit.tokensPerPeriod(1).
					period(Duration.ofMinutes(10)).
					refillSize(1).
					userData("second").
					build()).
			build();

		ConsumptionResult consumptionResult = bucket.tryConsume(1);
		List<Limit> bottleneck = consumptionResult.getBottleneck();
		requireThat(bottleneck, "bottleneck").size().isEqualTo(1);
		Limit limit = bottleneck.get(0);
		requireThat(limit.getUserData(), "limit.getUserData()").isEqualTo("second");
	}

	@Test
	public void updateTokensPerPeriod()
	{
		Bucket bucket = Bucket.builder().
			addLimit(Builder::build).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		limit.lastRefilledAt = Instant.now();
		Instant requestAt = limit.lastRefilledAt;
		Duration ONE_SECOND = Duration.ofSeconds(1);
		long expectedTokens = 0;
		for (int i = 1; i <= 10; ++i)
		{
			requestAt = requestAt.plus(ONE_SECOND);
			limit.refill(requestAt);
			expectedTokens += i;
			limit.updateConfiguration().
				tokensPerPeriod(i + 1).
				apply();
			requireThat(limit.availableTokens, "limit.availableTokens").isEqualTo(expectedTokens);
		}
	}
}