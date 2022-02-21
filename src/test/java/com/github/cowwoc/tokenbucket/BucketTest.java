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

	@Test
	public void roundingError()
	{
		int tokens = 9;
		int seconds = 10;
		Bucket bucket = Bucket.builder().
			addLimit(limit ->
				limit.tokensPerPeriod(tokens).
					period(Duration.ofSeconds(seconds)).
					minimumToRefill(1).
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
		requireThat(limit.getTokensAvailable(), "limit.getTokensAvailable()").
			isEqualTo(limit.getTokensPerPeriod());
	}
}
