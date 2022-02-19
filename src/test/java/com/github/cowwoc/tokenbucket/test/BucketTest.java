package com.github.cowwoc.tokenbucket.test;

import com.github.cowwoc.tokenbucket.Bucket;
import com.github.cowwoc.tokenbucket.BucketList;
import com.github.cowwoc.tokenbucket.SchedulingPolicy;
import org.testng.annotations.Test;

import java.time.Duration;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class BucketTest
{
	@Test
	public void bucketIsUnique()
	{
		Bucket first = new Bucket();
		Bucket second = new Bucket();
		requireThat(first, "first").isNotEqualTo(second, "second");
		requireThat(first.hashCode(), "first.hashCode()").isNotEqualTo(second.hashCode(), "second.hashCode()");
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void impossibleToConsume()
	{
		Bucket first = new Bucket();

		first.addLimit(5, Duration.ofSeconds(1)).
			initialTokens(5).
			maxTokens(5).
			minimumToRefill(1).
			apply();

		Bucket second = new Bucket();
		second.addLimit(5, Duration.ofSeconds(1)).
			initialTokens(10).
			minimumToRefill(1).
			apply();

		BucketList bucketList = new BucketList(SchedulingPolicy.roundRobin());
		//noinspection ResultOfMethodCallIgnored
		bucketList.tryConsume(10);
	}
}
