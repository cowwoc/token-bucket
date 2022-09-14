package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.internal.AbstractContainer;
import com.github.cowwoc.tokenbucket.internal.ContainerSecrets;
import com.github.cowwoc.tokenbucket.internal.SharedSecrets;

import java.util.List;

/**
 * Indicates how to consume tokens from children containers.
 */
public enum ConsumptionPolicy
{
	/**
	 * Consumes tokens from one child at a time.
	 */
	CONSUME_FROM_ONE
		{
			@Override
			long getMaximumTokens(ContainerList containerList)
			{
				List<AbstractContainer> descendants = CONTAINER_SECRETS.getDescendants(containerList);
				if (descendants.isEmpty())
					return 0;
				long maximumTokens = 0;
				for (AbstractContainer descendant : descendants)
				{
					if (descendant instanceof Bucket bucket)
						maximumTokens = Math.max(maximumTokens, bucket.getMaximumTokens());
				}
				return maximumTokens;
			}
		},
	/**
	 * Consumes tokens from all children at the same time.
	 */
	CONSUME_FROM_ALL
		{
			@Override
			long getMaximumTokens(ContainerList containerList)
			{
				List<AbstractContainer> descendants = CONTAINER_SECRETS.getDescendants(containerList);
				if (descendants.isEmpty())
					return 0;
				long maximumTokens = Long.MAX_VALUE;
				for (AbstractContainer descendant : descendants)
				{
					if (descendant instanceof Bucket bucket)
						maximumTokens = Math.min(maximumTokens, bucket.getMaximumTokens());
				}
				return maximumTokens;
			}
		};

	private static final ContainerSecrets CONTAINER_SECRETS = SharedSecrets.INSTANCE.containerSecrets;

	/**
	 * Returns the maximum number of tokens that the container can ever hold.
	 *
	 * @param containerList a ContainerList
	 * @return the maximum number of tokens that the container can ever hold
	 */
	abstract long getMaximumTokens(ContainerList containerList);
}