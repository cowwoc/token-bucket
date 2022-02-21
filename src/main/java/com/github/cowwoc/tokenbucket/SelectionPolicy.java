package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.internal.AbstractContainer;

import java.util.List;
import java.util.function.Function;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * Selects a bucket from a list.
 */
@SuppressWarnings("ClassCanBeRecord")
public final class SelectionPolicy
{
	private final Function<List<AbstractContainer>, AbstractContainer> nextBucket;

	/**
	 * @param nextBucket returns the next bucket to perform a task
	 * @throws NullPointerException if {@code nextBucket} is null
	 */
	private SelectionPolicy(Function<List<AbstractContainer>, AbstractContainer> nextBucket)
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
	public static SelectionPolicy roundRobin()
	{
		return new SelectionPolicy(new Function<>()
		{
			private int index = 0;

			@Override
			public AbstractContainer apply(List<AbstractContainer> buckets)
			{
				AbstractContainer bucket = buckets.get(index);
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
	AbstractContainer nextBucket(List<AbstractContainer> buckets)
	{
		assertThat(buckets, "buckets").isNotEmpty();
		return nextBucket.apply(buckets);
	}
}