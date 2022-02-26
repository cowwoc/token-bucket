package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.internal.AbstractContainer;

import java.util.List;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;

/**
 * Selects a bucket from a list.
 */
public enum SelectionPolicy
{
	ROUND_ROBIN
		{
			private int index = 0;

			@Override
			AbstractContainer nextContainer(List<AbstractContainer> containers)
			{
				assertThat(containers, "containers").isNotEmpty();
				AbstractContainer bucket = containers.get(index);
				// Wrap around end of list
				index = (index + 1) % containers.size();
				return bucket;
			}
		};

	/**
	 * Selects the next container to perform a task.
	 *
	 * @param containers a list of container
	 * @return the next container to perform a task
	 * @throws NullPointerException     if {@code containers} is null
	 * @throws IllegalArgumentException if {@code containers} is empty
	 */
	abstract AbstractContainer nextContainer(List<AbstractContainer> containers);
}