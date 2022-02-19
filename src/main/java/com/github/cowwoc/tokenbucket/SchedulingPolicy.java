package com.github.cowwoc.tokenbucket;

import java.util.List;
import java.util.function.Function;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * Selects the next bucket to perform a task.
 */
@SuppressWarnings("ClassCanBeRecord")
public final class SchedulingPolicy
{
	private final Function<List<Bucket>, Bucket> nextBucket;

	/**
	 * @param nextBucket returns the next bucket to perform a task
	 * @throws NullPointerException if {@code nextBucket} is null
	 */
	private SchedulingPolicy(Function<List<Bucket>, Bucket> nextBucket)
	{
		requireThat(nextBucket, "nextBucket").isNotNull();
		this.nextBucket = nextBucket;
	}

	/**
	 * Returns a scheduler that selects the next bucket in a round-robin fashion.
	 *
	 * @return a scheduler that selects the next bucket in a round-robin fashion
	 * @see <a href="https://en.wikipedia.org/wiki/Round-robin_scheduling">Round robin scheduling</a>
	 */
	public static SchedulingPolicy roundRobin()
	{
		return new SchedulingPolicy(new Function<>()
		{
			private int index = 0;

			@Override
			public Bucket apply(List<Bucket> buckets)
			{
				Bucket bucket = buckets.get(index);
				// Wrap around end of list
				index = (index + 1) % buckets.size();
				return bucket;
			}
		});
	}

	/**
	 * Selects the next bucket to perform a task.
	 *
	 * @param buckets a list of buckets
	 * @return the next bucket to perform a task
	 * @throws NullPointerException     if {@code buckets} is null
	 * @throws IllegalArgumentException if {@code buckets} is empty
	 */
	Bucket nextBucket(List<Bucket> buckets)
	{
		assertThat(buckets, "buckets").isNotEmpty();
		return nextBucket.apply(buckets);
	}
}
