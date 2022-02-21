package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.Requirements;
import org.testng.annotations.Test;

public final class ContainerListTest
{
	@Test
	public void consumeFromOneRoundRobin()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.roundRobin()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.initialTokens(5).
							userData("firstLimit").
							build()).
					userData("firstBucket").
					build()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.initialTokens(10).
							userData("secondLimit").
							build()).
					userData("secondBucket").
					build()).
			build();

		Requirements requirements = new Requirements();
		Bucket first = (Bucket) containerList.getChildren().stream().
			filter(bucket -> bucket.getUserData().equals("firstBucket")).
			findFirst().orElse(null);
		requirements.requireThat(first, "first").isNotNull();
		Bucket second = (Bucket) containerList.getChildren().stream().
			filter(bucket -> bucket.getUserData().equals("secondBucket")).
			findFirst().orElse(null);
		requirements.requireThat(second, "second").isNotNull();

		ConsumptionResult consumptionResult = containerList.tryConsume(10);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").
			isTrue();
		requirements.requireThat(consumptionResult.getContainer(), "consumptionResult.getContainer()").
			isEqualTo(second);
		consumptionResult = containerList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").
			isTrue();
		requirements.requireThat(consumptionResult.getContainer(), "consumptionResult.getContainer()").
			isEqualTo(first);
		consumptionResult = containerList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").
			isFalse();
	}

	@Test
	public void consumeFromAll()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.roundRobin()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.initialTokens(5).
							userData("first").
							build()).
					build()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.initialTokens(5).
							userData("second").
							build()).
					build()).
			consumeFromAll().
			build();

		ConsumptionResult consumptionResult = containerList.tryConsume(5);
		Requirements requirements = new Requirements();
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isTrue();
		requirements.requireThat(consumptionResult.getContainer(), "consumptionResult.getContainer()").
			isEqualTo(containerList);
		for (Container child : containerList.getChildren())
		{
			Bucket bucket = (Bucket) child;
			requirements.requireThat(bucket.getTokensAvailable(), "bucket.getTokensAvailable()").isEqualTo(0L);
		}
		consumptionResult = containerList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isFalse();
	}
}
