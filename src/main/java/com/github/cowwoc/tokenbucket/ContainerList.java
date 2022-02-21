package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.requirements.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.AbstractContainer;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.ConsumptionPolicy;
import com.github.cowwoc.tokenbucket.internal.ContainerSecrets;
import com.github.cowwoc.tokenbucket.internal.ReadWriteLockAsResource;
import com.github.cowwoc.tokenbucket.internal.SharedSecrets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A container of one or more children containers.
 * <p>
 * For example, this allows to consume tokens from a list of buckets.
 */
public final class ContainerList extends AbstractContainer
{
	private static final ContainerSecrets CONTAINER_SECRETS = SharedSecrets.INSTANCE.containerSecrets;
	private static final Function<SelectionPolicy, ConsumptionPolicy> CONSUME_FROM_ONE_POLICY =
		selectionPolicy ->
			(minimumTokens, maximumTokens, requestedAt, abstractBucket) ->
			{
				ContainerList containerList = (ContainerList) abstractBucket;
				AbstractContainer firstBucket = null;
				ConsumptionResult earliestConsumption = null;
				try (CloseableLock ignored = containerList.lock.writeLock())
				{
					List<AbstractContainer> buckets = containerList.children;
					assertThat(buckets, "buckets").isNotEmpty();
					while (true)
					{
						AbstractContainer bucket = selectionPolicy.nextBucket(buckets);
						ConsumptionResult consumptionResult = CONTAINER_SECRETS.tryConsumeRange(bucket, minimumTokens,
							maximumTokens, requestedAt);
						if (consumptionResult.isSuccessful())
							return consumptionResult;
						if (bucket == firstBucket)
							return earliestConsumption;
						if (firstBucket == null)
						{
							firstBucket = bucket;
							earliestConsumption = consumptionResult;
						}
						else if (earliestConsumption.getTokensAvailableAt().isAfter(consumptionResult.getTokensAvailableAt()))
							earliestConsumption = consumptionResult;
					}
				}
			};
	private static final ConsumptionPolicy CONSUME_FROM_ALL_POLICY =
		(minimumTokens, maximumTokens, requestedAt, abstractBucket) ->
		{
			ContainerList containerList = (ContainerList) abstractBucket;
			try (CloseableLock ignored = containerList.lock.writeLock())
			{
				List<AbstractContainer> buckets = containerList.children;
				assertThat(buckets, "buckets").isNotEmpty();
				long tokensConsumed = maximumTokens;
				for (AbstractContainer bucket : buckets)
					tokensConsumed = Math.min(tokensConsumed, CONTAINER_SECRETS.getTokensAvailable(bucket));
				if (tokensConsumed < minimumTokens)
				{
					return new ConsumptionResult(containerList, minimumTokens, maximumTokens, 0,
						requestedAt, requestedAt);
				}

				for (AbstractContainer bucket : buckets)
				{
					ConsumptionResult consumptionResult = CONTAINER_SECRETS.tryConsumeRange(bucket, tokensConsumed,
						tokensConsumed, requestedAt);
					assertThat(consumptionResult.isSuccessful(), "consumptionResult").isTrue();
				}
				return new ConsumptionResult(containerList, minimumTokens, maximumTokens, tokensConsumed,
					requestedAt, requestedAt);
			}
		};

	/**
	 * Builds a bucket that contains children buckets.
	 * <p>
	 * By default, the list will delegate to the first child that has sufficient number of tokens available.
	 *
	 * @return a new ContainerList builder
	 */
	public static Builder builder()
	{
		return new Builder(new ReadWriteLockAsResource(), list ->
		{
		});
	}

	private List<AbstractContainer> children;
	private final Logger log = LoggerFactory.getLogger(ContainerList.class);

	/**
	 * Creates a new ContainerList.
	 *
	 * @param lock              the lock over the list's state
	 * @param consumptionPolicy indicates how tokens are consumed
	 * @param children          the children in this list
	 * @throws NullPointerException     if any of the arguments are null
	 * @throws IllegalArgumentException if {@code children} are empty
	 */
	private ContainerList(List<AbstractContainer> children, ReadWriteLockAsResource lock,
	                      ConsumptionPolicy consumptionPolicy)
	{
		super(lock, consumptionPolicy);
		assertThat(children, "children").isNotEmpty();
		this.children = children;
	}

	@Override
	protected void updateChild(Object child, Runnable update)
	{
		AbstractContainer childContainer = (AbstractContainer) child;
		children.remove(childContainer);

		update.run();

		children.add(childContainer);
	}

	@Override
	protected Logger getLogger()
	{
		return log;
	}

	/**
	 * Returns the children in this list.
	 *
	 * @return an unmodifiable list
	 */
	public List<Container> getChildren()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return List.copyOf(children);
		}
	}

	@Override
	protected long getTokensAvailable()
	{
		if (children.isEmpty())
			return 0;
		long tokensAvailable = Long.MAX_VALUE;
		for (AbstractContainer child : children)
			tokensAvailable = Math.min(tokensAvailable, CONTAINER_SECRETS.getTokensAvailable(child));
		return tokensAvailable;
	}

	/**
	 * Returns the number of children in this list. If this list contains more than {@code Integer.MAX_VALUE}
	 * children, returns {@code Integer.MAX_VALUE}.
	 *
	 * @return the number of children in this list
	 */
	public int size()
	{
		try (CloseableLock ignored = lock.readLock())
		{
			return children.size();
		}
	}

	@Override
	public String toString()
	{
		return "children: " + children;
	}

	/**
	 * Updates this list's configuration.
	 * <p>
	 * Please note that users are allowed to consume tokens between the time this method is invoked and
	 * {@link ConfigurationUpdater#apply()} completes. Users who wish to add/remove a relative amount of
	 * tokens should avoid accessing the list or its descendants until the configuration update is complete.
	 *
	 * @return the configuration updater
	 */
	public ConfigurationUpdater updateConfiguration()
	{
		return new ConfigurationUpdater();
	}

	/**
	 * Builds a ContainerList.
	 */
	public static final class Builder
	{
		private final ReadWriteLockAsResource lock;
		private final Consumer<ContainerList> consumer;
		private ConsumptionPolicy consumptionPolicy;
		private final List<AbstractContainer> children = new ArrayList<>();

		/**
		 * Creates a new builder.
		 * <p>
		 * By default, the list will delegate to the first child that has sufficient number of tokens available,
		 * using the round-robin scheduling policy.
		 *
		 * @param lock     the lock over the bucket's state
		 * @param consumer consumes the ContainerList before it is returned
		 * @throws NullPointerException if any of the arguments are null
		 */
		private Builder(ReadWriteLockAsResource lock, Consumer<ContainerList> consumer)
		{
			assertThat(lock, "lock").isNotNull();
			assertThat(consumer, "consumer").isNotNull();
			this.lock = lock;
			this.consumer = consumer;
			consumeFromOne(SelectionPolicy.roundRobin());
		}

		/**
		 * Indicates that the list should delegate to the first child that has sufficient number of tokens
		 * available.
		 *
		 * @param selectionPolicy determines the order in which buckets are evaluated
		 * @return this
		 * @throws NullPointerException if {@code selectionPolicy} is null
		 */
		public Builder consumeFromOne(SelectionPolicy selectionPolicy)
		{
			requireThat(selectionPolicy, "selectionPolicy").isNotNull();
			this.consumptionPolicy = CONSUME_FROM_ONE_POLICY.apply(selectionPolicy);
			return this;
		}

		/**
		 * Indicates that the list should consume tokens from all children simultaneously.
		 *
		 * @return this
		 */
		public Builder consumeFromAll()
		{
			this.consumptionPolicy = CONSUME_FROM_ALL_POLICY;
			return this;
		}

		/**
		 * Adds a Bucket to this list.
		 *
		 * @param bucketBuilder builds the Bucket
		 * @return this
		 * @throws NullPointerException if {@code bucketBuilder} is null
		 */
		@CheckReturnValue
		public Builder addBucket(Consumer<Bucket.Builder> bucketBuilder)
		{
			requireThat(bucketBuilder, "bucketBuilder").isNotNull();
			bucketBuilder.accept(new Bucket.Builder(lock, children::add));
			return this;
		}

		/**
		 * Adds a ContainerList to this list.
		 *
		 * @param listBuilder builds the ContainerList
		 * @return this
		 * @throws NullPointerException if {@code listBuilder} is null
		 */
		@CheckReturnValue
		public Builder addContainerList(Consumer<Builder> listBuilder)
		{
			requireThat(listBuilder, "listBuilder").isNotNull();
			listBuilder.accept(new Builder(lock, children::add));
			return this;
		}

		/**
		 * Builds a new ContainerList.
		 *
		 * @return a new ContainerList
		 * @throws IllegalArgumentException if {@code buckets} is empty
		 */
		public ContainerList build()
		{
			ContainerList containerList = new ContainerList(children, lock, consumptionPolicy);
			consumer.accept(containerList);
			return containerList;
		}
	}

	/**
	 * Updates this ContainerList's configuration.
	 * <p>
	 * <b>Thread-safety</b>: This class is not thread-safe.
	 */
	public final class ConfigurationUpdater
	{
		private ConsumptionPolicy consumptionPolicy;
		private final List<AbstractContainer> children;
		private final Consumer<AbstractContainer> addChild;
		private boolean wakeConsumers;
		private boolean changed;

		/**
		 * Creates a new configuration updater.
		 */
		private ConfigurationUpdater()
		{
			try (CloseableLock ignored = lock.readLock())
			{
				this.children = new ArrayList<>(ContainerList.this.children);
			}
			this.addChild = child ->
			{
				if (consumptionPolicy == CONSUME_FROM_ONE_POLICY)
				{
					// Adding a new bucket could allow consumers to consume tokens earlier than anticipated
					wakeConsumers = true;
				}
				children.add(child);
			};
		}

		/**
		 * Indicates that the list should delegate to the first child that has sufficient number of tokens
		 * available.
		 *
		 * @param selectionPolicy determines the order in which buckets are evaluated
		 * @return this
		 * @throws NullPointerException if {@code selectionPolicy} is null
		 */
		public ConfigurationUpdater consumeFromOne(SelectionPolicy selectionPolicy)
		{
			requireThat(selectionPolicy, "selectionPolicy").isNotNull();
			ConsumptionPolicy consumptionPolicy = CONSUME_FROM_ONE_POLICY.apply(selectionPolicy);
			// This equality check will probably always return false but it's worth a try
			if (this.consumptionPolicy == consumptionPolicy)
				return this;
			changed = true;
			this.consumptionPolicy = consumptionPolicy;
			return this;
		}

		/**
		 * Indicates that the list should consume tokens from all children simultaneously.
		 *
		 * @return this
		 */
		public ConfigurationUpdater consumeFromAll()
		{
			if (this.consumptionPolicy == CONSUME_FROM_ALL_POLICY)
				return this;
			changed = true;
			this.consumptionPolicy = CONSUME_FROM_ALL_POLICY;
			return this;
		}

		/**
		 * Adds a Bucket to this list.
		 *
		 * @param bucketBuilder builds the Bucket
		 * @return this
		 * @throws NullPointerException if {@code bucketBuilder} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater addBucket(Consumer<Bucket.Builder> bucketBuilder)
		{
			requireThat(bucketBuilder, "bucketBuilder").isNotNull();
			bucketBuilder.accept(new Bucket.Builder(lock, addChild::accept));
			changed = true;
			return this;
		}

		/**
		 * Adds a ContainerList to this list.
		 *
		 * @param listBuilder builds the ContainerList
		 * @return this
		 * @throws NullPointerException if {@code listBuilder} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater addContainerList(Consumer<Builder> listBuilder)
		{
			requireThat(listBuilder, "listBuilder").isNotNull();
			listBuilder.accept(new Builder(lock, addChild::accept));
			changed = true;
			return this;
		}

		/**
		 * Removes a child from this list.
		 *
		 * @param child a child
		 * @return this
		 * @throws NullPointerException if {@code child} is null
		 */
		@CheckReturnValue
		public ConfigurationUpdater remove(Container child)
		{
			requireThat(child, "child").isNotNull();
			if (children.remove((AbstractContainer) child))
				changed = true;
			return this;
		}

		/**
		 * Removes all children from this list.
		 *
		 * @return this
		 */
		@CheckReturnValue
		public ConfigurationUpdater clear()
		{
			if (children.isEmpty())
				return this;
			changed = true;
			children.clear();
			return this;
		}

		/**
		 * Updates this ContainerList's configuration.
		 *
		 * @throws IllegalArgumentException if {@code limits} is empty
		 */
		public void apply()
		{
			requireThat(ContainerList.this.children, "children").isNotEmpty();
			if (!changed)
				return;
			try (CloseableLock ignored = lock.writeLock())
			{
				parent.updateChild(ContainerList.this, () -> ContainerList.this.children = children);
				if (wakeConsumers)
					tokensUpdated.signalAll();
			}
		}
	}
}