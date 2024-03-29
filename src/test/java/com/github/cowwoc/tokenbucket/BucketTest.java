package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.Limit.Builder;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
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
	public void negativeInitialTokens()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.initialTokens(-100).
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
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		requireThat(limit.availableTokens, "limit.getAvailableTokens()").isPositive();
		ConsumptionResult consumptionResult = bucket.tryConsume();
		requireThat(consumptionResult.getAvailableIn(), "consumptionResult.getAvailableIn()").
			isEqualTo(Duration.ZERO);

		try (Limit.ConfigurationUpdater update = limit.updateConfiguration())
		{
			update.availableTokens(-100);
		}
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
					build()).
			addLimit(limit ->
				limit.initialTokens(100).
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
		limit.refill(limit.startOfCurrentPeriod.plusSeconds(5));
		long tokensAfter = limit.availableTokens;
		long tokensAdded = tokensAfter - tokensBefore;
		requireThat(tokensAdded, "tokensAdded").isEqualTo(4L);
	}

	@Test
	public void bottleneckListIsMinimal()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.userData("first").
					period(Duration.ofMinutes(1)).
					build()).
			addLimit(limit ->
				limit.period(Duration.ofMinutes(10)).
					userData("second").
					build()).
			build();

		ConsumptionResult consumptionResult = bucket.tryConsume(1);
		List<Limit> bottleneck = consumptionResult.getBottlenecks();
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
		Duration ONE_SECOND = Duration.ofSeconds(1);
		long expectedTokens = 0;
		for (int i = 1; i <= 10; ++i)
		{
			Instant requestedAt = limit.startOfCurrentPeriod.plus(ONE_SECOND);
			limit.refill(requestedAt);
			expectedTokens += i;
			try (Limit.ConfigurationUpdater update = limit.updateConfiguration())
			{
				update.tokensPerPeriod(i + 1);
			}
			requireThat(limit.availableTokens, "limit.availableTokens").isEqualTo(expectedTokens);
		}
	}

	@Test
	public void limitUpdateConfigurationRetainsOrder()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.period(Duration.ofMinutes(1)).
					userData("first").
					build()).
			addLimit(limit ->
				limit.period(Duration.ofMinutes(10)).
					userData("second").
					build()).
			build();

		List<Object> oldOrder = new ArrayList<>(bucket.getLimits().stream().map(Limit::getUserData).toList());
		for (Limit limit : bucket.getLimits())
		{
			try (Limit.ConfigurationUpdater update = limit.updateConfiguration())
			{
				update.tokensPerPeriod(update.tokensPerPeriod() + 1);
			}
			List<Object> newOrder = bucket.getLimits().stream().map(Limit::getUserData).toList();
			requireThat(newOrder, "newOrder").isEqualTo(oldOrder, "oldOrder");
		}
	}

	@Test
	public void limitWithLowestRefillRate()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.period(Duration.ofMinutes(1)).
					userData("first").
					build()).
			addLimit(limit ->
				limit.period(Duration.ofMinutes(100)).
					userData("second").
					build()).
			addLimit(limit ->
				limit.period(Duration.ofMinutes(50)).
					userData("third").
					build()).
			build();

		Limit lowestRefillRate = bucket.getLimitWithLowestRefillRate();
		requireThat(lowestRefillRate.getUserData(), "lowestRefillRate.getUserData()").isEqualTo("second");
	}

	@Test
	public void roundRobinSupportsReductionOfContainerSize() throws InterruptedException
	{
		// SelectionPolicy.ROUND_ROBIN would sometimes reference non-existent children if the number of children
		// was decreased.
		ContainerList containerList = ContainerList.builder().
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.initialTokens(10).
							tokensPerPeriod(1).
							period(Duration.ofSeconds(1)).
							build()).
					userData("bucket1").
					build()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.initialTokens(10).
							tokensPerPeriod(1).
							period(Duration.ofSeconds(1)).
							build()).
					userData("bucket2").
					build()).
			build();

		// Selection index = 0
		ConsumptionResult ignored = containerList.consume();
		// Selection index = 1

		// Reduce the number of children from 2 to 1
		try (ContainerList.ConfigurationUpdater updater = containerList.updateConfiguration())
		{
			updater.remove(containerList.getChildren().get(0));
		}

		// Cannot select index 1 since the child was removed. A selection index of 0 should be used.
		ignored = containerList.consume();
	}
}