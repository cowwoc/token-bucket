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
				List<AbstractContainer> children = containerList.children;
				if (children.isEmpty())
					return 0;
				long maximumTokens = 0;
				for (AbstractContainer child : children)
					maximumTokens = Math.max(maximumTokens, CONTAINER_SECRETS.getMaximumTokens(child));
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
				List<AbstractContainer> children = containerList.children;
				if (children.isEmpty())
					return 0;
				long maximumTokens = Long.MAX_VALUE;
				for (AbstractContainer child : children)
					maximumTokens = Math.min(maximumTokens, CONTAINER_SECRETS.getMaximumTokens(child));
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