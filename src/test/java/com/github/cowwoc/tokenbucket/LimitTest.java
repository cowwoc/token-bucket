package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.Requirements;
import com.github.cowwoc.tokenbucket.Limit.ConfigurationUpdater;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class LimitTest
{
	@Test
	public void updateConfiguration()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit -> limit.userData("limit").build()).
			build();
		List<Limit> limits = bucket.getLimits();
		requireThat(limits, "limits").size().isEqualTo(1);
		Limit limit = limits.iterator().next();
		requireThat(limit.getTokensPerPeriod(), "limit.getTokensPerPeriod()").isEqualTo(1L);
		requireThat(limit.getUserData(), "limit.getUserData()").isEqualTo("limit");

		try (ConfigurationUpdater update = limit.updateConfiguration())
		{
			update.tokensPerPeriod(5);
		}
		requireThat(limit.getTokensPerPeriod(), "limit.getTokensPerPeriod()").isEqualTo(5L);
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
					build()).
			build();

		Limit limit = bucket.getLimits().iterator().next();
		Instant requestedAt = limit.startOfCurrentPeriod;
		Duration timeIncrement = limit.getPeriod().dividedBy(seconds);
		Requirements requirements = new Requirements();
		for (int i = 1; i <= seconds; ++i)
		{
			requirements.withContext("i", i);
			requestedAt = requestedAt.plus(timeIncrement);
			limit.refill(requestedAt);
			requirements.requireThat(limit.availableTokens, "limit.availableTokens").
				isEqualTo((long) ((double) tokens * i / seconds));
		}
		requirements.withoutContext("i").
			requireThat(limit.availableTokens, "limit.availableTokens").
			isEqualTo(limit.getTokensPerPeriod());

		for (int i = 1; i <= seconds; ++i)
		{
			requestedAt = requestedAt.plus(timeIncrement);
			limit.refill(requestedAt);
			requirements.requireThat(limit.availableTokens, "limit.availableTokens").
				isEqualTo(tokens + (long) ((double) tokens * i / seconds));
		}
		requirements.requireThat(limit.availableTokens, "limit.availableTokens").
			isEqualTo(2 * limit.getTokensPerPeriod());
	}

	@Test
	public void maxTokensIsNotMultipleOfRefillSize()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit -> limit.
				tokensPerPeriod(60).
				period(Duration.ofMinutes(1)).
				maximumTokens(120).
				refillSize(50).
				build()).
			build();
		List<Limit> limits = bucket.getLimits();
		requireThat(limits, "limits").size().isEqualTo(1);
		Limit limit = limits.iterator().next();
		limit.refill(limit.startOfCurrentPeriod.plusSeconds(50));
		requireThat(limit.availableTokens, "limit.availableTokens").isEqualTo(50L);
		limit.consume(50);
		requireThat(limit.availableTokens, "limit.availableTokens").isEqualTo(0L);

		try (ConfigurationUpdater update = limit.updateConfiguration())
		{
			update.tokensPerPeriod(30).
				period(Duration.ofSeconds(30));
		}
		limit.refill(limit.startOfCurrentPeriod.plusSeconds(50));
		requireThat(limit.availableTokens, "limit.availableTokens").isEqualTo(30L);
	}

	@Test
	public void updateRefillSize()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit -> limit.
				tokensPerPeriod(60).
				period(Duration.ofSeconds(60)).
				maximumTokens(120).
				refillSize(10).
				build()).
			build();
		List<Limit> limits = bucket.getLimits();
		requireThat(limits, "limits").size().isEqualTo(1);
		Limit limit = limits.iterator().next();
		Instant consumedAt = limit.startOfCurrentPeriod.plusSeconds(30);
		ConsumptionResult consumptionResult = bucket.tryConsume(30, consumedAt);
		requireThat(consumptionResult.getTokensLeft(), "consumptionResult.getTokensLeft()").isEqualTo(0L);

		try (ConfigurationUpdater update = limit.updateConfiguration())
		{
			update.refillSize(20);
		}
		requireThat(limit.availableTokens, "limit.availableTokens").isEqualTo(0L);
		limit.refill(limit.startOfCurrentPeriod.plusSeconds(30));
		requireThat(limit.availableTokens, "limit.availableTokens").isEqualTo(20L);
	}

	/**
	 * Ensure that modifying the configuration updates refillsPerPeriod.
	 */
	@Test
	public void refillsPerPeriodNotUpdated()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit -> limit.
				tokensPerPeriod(120).
				period(Duration.ofMinutes(1)).
				maximumTokens(120).
				refillSize(60).
				build()).
			build();
		List<Limit> limits = bucket.getLimits();
		requireThat(limits, "limits").size().isEqualTo(1);
		Limit limit = limits.iterator().next();
		ConsumptionResult result = bucket.tryConsume(1, limit.startOfCurrentPeriod);
		requireThat(result.isSuccessful(), "result.isSuccessful()").isFalse();
		requireThat(result.getTokensLeft(), "result.getTokensLeft()").isEqualTo(0L);

		result = bucket.tryConsume(60, result.getConsumeAt());
		requireThat(result.getTokensLeft(), "result.getTokensLeft()").isEqualTo(0L);

		try (ConfigurationUpdater update = limit.updateConfiguration())
		{
			update.refillSize(1);
		}

		result = bucket.tryConsume(30, result.getConsumeAt());
		Instant availableAt = result.getAvailableAt();
		limit.refill(availableAt);
		result = bucket.tryConsume(30, availableAt);

		requireThat(result.getTokensLeft(), "result.getTokensLeft()").isEqualTo(0L);
	}
}