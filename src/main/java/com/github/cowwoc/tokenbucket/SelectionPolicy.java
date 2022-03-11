package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.internal.AbstractContainer;
import com.github.cowwoc.tokenbucket.internal.ContainerSelector;

import java.util.List;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;

/**
 * Selects a bucket from a list.
 */
public enum SelectionPolicy
{
	/**
	 * A scheduler that selects the next bucket in a round-robin fashion.
	 *
	 * @see <a href="https://en.wikipedia.org/wiki/Round-robin_scheduling">Round robin scheduling</a>
	 */
	ROUND_ROBIN
		{
			@Override
			ContainerSelector createSelector()
			{
				return new ContainerSelector()
				{
					private int index = -1;

					@Override
					public AbstractContainer nextContainer(List<AbstractContainer> containers)
					{
						assertThat(containers, "containers").isNotEmpty();
						// Wrap around end of list
						index = (index + 1) % containers.size();
						return containers.get(index);
					}
				};
			}
		};

	/**
	 * @return a new {@code ContainerSelector} that implements this policy
	 */
	abstract ContainerSelector createSelector();
}