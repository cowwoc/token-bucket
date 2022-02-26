package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.Requirements;
import com.github.cowwoc.tokenbucket.Limit.Builder;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class ContainerListTest
{
	@Test
	public void consumeFromOneRoundRobin()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
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
	public void consumeFromOneWhenSomeBucketsWillNeverHaveEnoughTokens()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.maximumTokens(5).
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
		consumptionResult = containerList.tryConsume(10);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").
			isFalse();
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void consumeFromOneWhenAllBucketsWillNeverHaveEnoughTokens()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.maximumTokens(5).
							userData("firstLimit").
							build()).
					userData("firstBucket").
					build()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.maximumTokens(5).
							userData("secondLimit").
							build()).
					userData("secondBucket").
					build()).
			build();

		//noinspection ResultOfMethodCallIgnored
		containerList.tryConsume(10);
	}

	@Test
	public void consumeFromAll()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
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
			requirements.requireThat(bucket.getAvailableTokens(), "bucket.getAvailableTokens()").isEqualTo(0L);
		}
		consumptionResult = containerList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isFalse();
	}

	@Test
	public void containerOfContainersConsumeFromOne()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
			addContainerList(child -> child.
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(10).
								userData("limit1").
								build()).
						userData("bucket1").
						build()).
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(10).
								userData("limit1").
								build()).
						userData("bucket2").
						build()).
				consumeFromAll().
				userData("list1").
				build()).
			addContainerList(child -> child.
				addBucket(bucket -> bucket.addLimit(limit ->
						limit.initialTokens(5).
							userData("limit3").
							build()).
					userData("bucket3").
					build()).
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit4").
								build()).
						userData("bucket4").
						build()).
				consumeFromAll().
				userData("list2").
				build()).build();

		ConsumptionResult consumptionResult = containerList.tryConsume(10);
		Requirements requirements = new Requirements();
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isTrue();

		ContainerList list1 = containerList.getChildren().stream().filter(child -> child.getUserData().equals("list1")).
			map(container -> (ContainerList) container).
			findFirst().orElse(null);

		assert (list1 != null);
		requirements.requireThat(consumptionResult.getContainer(), "consumptionResult.getContainer()").
			isEqualTo(list1);
		for (Container child : list1.getChildren())
		{
			Bucket bucket = (Bucket) child;
			requirements.requireThat(bucket.getAvailableTokens(), "bucket.getAvailableTokens()").isEqualTo(0L);
		}

		ContainerList list2 = containerList.getChildren().stream().filter(child -> child.getUserData().equals("list2")).
			map(container -> (ContainerList) container).
			findFirst().orElse(null);
		assert (list2 != null);
		for (Container child : list2.getChildren())
		{
			Bucket bucket = (Bucket) child;
			requirements.requireThat(bucket.getAvailableTokens(), "bucket.getAvailableTokens()").isEqualTo(5L);
		}

		consumptionResult = containerList.tryConsume(10);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isFalse();

		consumptionResult = containerList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isTrue();
	}

	@Test
	public void containerOfContainersConsumeFromAll()
	{
		ContainerList containerList = ContainerList.builder().
			addContainerList(child -> child.
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit1").
								build()).
						userData("bucket1").
						build()).
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit1").
								build()).
						userData("bucket2").
						build()).
				consumeFromAll().
				userData("list1").
				build()).
			addContainerList(child -> child.
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit3").
								build()).
						userData("bucket3").
						build()).
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit4").
								build()).
						userData("bucket4").
						build()).
				consumeFromAll().
				userData("list2").
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
			ContainerList childList = (ContainerList) child;
			requirements.requireThat(childList.getAvailableTokens(), "childList.getAvailableTokens()").isEqualTo(0L);
		}
		consumptionResult = containerList.tryConsume(5);
		requirements = requirements.withContext("consumptionResult", consumptionResult);
		requirements.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isFalse();
	}

	@Test
	public void consumeFromOneBottleneckListIsMinimal()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.period(Duration.ofMinutes(1)).
							userData("firstLimit").
							build()).
					userData("firstBucket").
					build()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.period(Duration.ofMinutes(10)).
							userData("secondLimit").
							build()).
					userData("secondBucket").
					build()).
			build();

		ConsumptionResult consumptionResult = containerList.tryConsume(1);
		List<Limit> bottleneck = consumptionResult.getBottlenecks();
		requireThat(bottleneck, "bottleneck").size().isEqualTo(1);
		Limit limit = bottleneck.get(0);
		requireThat(limit.getUserData(), "limit.getUserData()").isEqualTo("firstLimit");
	}

	@Test
	public void consumeFromAllBottleneckListIsMinimal()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromAll().
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.period(Duration.ofMinutes(1)).
							userData("firstLimit").
							build()).
					userData("firstBucket").
					build()).
			addBucket(bucket ->
				bucket.addLimit(limit ->
						limit.initialTokens(1).
							period(Duration.ofMinutes(10)).
							userData("secondLimit").
							build()).
					addLimit(limit ->
						limit.period(Duration.ofMinutes(1)).
							userData("thirdLimit").
							build()).
					userData("secondBucket").
					build()).
			build();

		ConsumptionResult consumptionResult = containerList.tryConsume(1);
		List<Limit> bottleneck = consumptionResult.getBottlenecks();
		requireThat(bottleneck, "bottleneck").size().isEqualTo(2);
		List<String> limitNames = bottleneck.stream().map(limit -> (String) limit.getUserData()).toList();
		requireThat(limitNames, "limitNames").isEqualTo(List.of("firstLimit", "thirdLimit"));
	}

	@Test
	public void bucketUpdateConfigurationRetainsOrder()
	{
		ContainerList containerList = ContainerList.builder().
			consumeFromOne(SelectionPolicy.ROUND_ROBIN).
			addBucket(bucket ->
				bucket.addLimit(Builder::build).
					userData("firstBucket").
					build()).
			addBucket(bucket ->
				bucket.addLimit(Builder::build).
					userData("secondBucket").
					build()).
			build();

		List<Object> oldOrder = new ArrayList<>(containerList.getChildren().stream().map(Container::getUserData).
			toList());
		for (Container child : containerList.getChildren())
		{
			Bucket bucket = (Bucket) child;
			try (Bucket.ConfigurationUpdater update = bucket.updateConfiguration())
			{
				update.addLimit(Builder::build);
			}
			List<Object> newOrder = containerList.getChildren().stream().map(Container::getUserData).toList();
			requireThat(newOrder, "newOrder").isEqualTo(oldOrder, "oldOrder");
		}
	}

	@Test
	public void containerListUpdateConfigurationRetainsOrder()
	{
		ContainerList containerList = ContainerList.builder().
			addContainerList(child -> child.
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit1").
								build()).
						userData("bucket1").
						build()).
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit1").
								build()).
						userData("bucket2").
						build()).
				consumeFromAll().
				userData("list1").
				build()).
			addContainerList(child -> child.
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit3").
								build()).
						userData("bucket3").
						build()).
				addBucket(bucket ->
					bucket.addLimit(limit ->
							limit.initialTokens(5).
								userData("limit4").
								build()).
						userData("bucket4").
						build()).
				consumeFromAll().
				userData("list2").
				build()).
			consumeFromAll().
			build();

		List<Object> oldOrder = new ArrayList<>(containerList.getChildren().stream().map(Container::getUserData).
			toList());
		for (Container child : containerList.getChildren())
		{
			ContainerList nestedList = (ContainerList) child;
			try (ContainerList.ConfigurationUpdater update = nestedList.updateConfiguration())
			{
				update.consumeFromOne(SelectionPolicy.ROUND_ROBIN);
			}
			List<Object> newOrder = containerList.getChildren().stream().map(Container::getUserData).toList();
			requireThat(newOrder, "newOrder").isEqualTo(oldOrder, "oldOrder");
		}
	}
}