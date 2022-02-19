package com.github.cowwoc.tokenbucket.test;

import com.github.cowwoc.requirements.Requirements;
import com.github.cowwoc.tokenbucket.Bucket;
import com.github.cowwoc.tokenbucket.BucketList;
import com.github.cowwoc.tokenbucket.ConsumptionResult;
import com.github.cowwoc.tokenbucket.SchedulingPolicy;
import org.testng.annotations.Test;

import java.time.Duration;

public final class BucketListTest
{
	@Test
	public void roundRobin()
	{
		Bucket first = new Bucket();

		first.addLimit(5, Duration.ofSeconds(10)).
			initialTokens(5).
			apply();

		Bucket second = new Bucket();
		second.addLimit(5, Duration.ofMinutes(10)).
			initialTokens(10).
			apply();

		BucketList bucketList = new BucketList(SchedulingPolicy.roundRobin());
		bucketList.add(first);
		bucketList.add(second);
		Requirements requirements = new Requirements();
		ConsumptionResult consumptionResult = bucketList.tryConsume(10);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").
			isTrue();
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.getBucket(), "consumptionResult.getBucket()").
			isEqualTo(second);
		consumptionResult = bucketList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").
			isTrue();
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.getBucket(), "consumptionResult.getBucket()").
			isEqualTo(first);
		consumptionResult = bucketList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").
			isFalse();
	}
}
