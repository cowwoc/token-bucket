package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.annotation.CheckReturnValue;
import com.github.cowwoc.tokenbucket.internal.AbstractContainer;
import com.github.cowwoc.tokenbucket.internal.CloseableLock;
import com.github.cowwoc.tokenbucket.internal.ConsumptionFunction;
import com.github.cowwoc.tokenbucket.internal.ContainerSecrets;
import com.github.cowwoc.tokenbucket.internal.ContainerSelector;
import com.github.cowwoc.tokenbucket.internal.SharedSecrets;
import com.github.cowwoc.tokenbucket.internal.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.github.cowwoc.requirements.DefaultRequirements.assertThat;
import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A container of one or more children containers.
 * <p>
 * For example, this allows to consume tokens from a list of buckets.
 * <p>
 * <b>Thread safety</b>: This class is thread-safe.
 */
public final class ContainerList extends AbstractContainer
{
	private static final ContainerSecrets CONTAINER_SECRETS = SharedSecrets.INSTANCE.containerSecrets;
	private static final Function<ContainerSelector, ConsumptionFunction> CONSUME_FROM_ONE_POLICY =
		selectionPolicy ->
			(minimumTokens, maximumTokens, nameOfMinimumTokens, requestedAt, consumedAt, abstractContainer) ->
			{
				ContainerList containerList = (ContainerList) abstractContainer;
				AbstractContainer firstContainer = null;
				ConsumptionResult earliestConsumption = null;

				while (true)
				{
					// Prevent the list of children from changing
					try (CloseableLock closeableLock = containerList.lock.readLock())
					{
						@SuppressWarnings("unchecked")
						List<AbstractContainer> children = (List<AbstractContainer>) (List<?>) containerList.getChildren();
						assertThat(r -> r.requireThat(children, "children").isNotEmpty());

						AbstractContainer container = selectionPolicy.nextContainer(children);
						if (container == firstContainer)
						{
							if (earliestConsumption == null)
							{
								long containerMaximumTokens = containerList.getMaximumTokens();
								throw new IllegalArgumentException("The maximum number of tokens that the container could " +
									"ever contain (containerMaximumTokens) is less than the minimum number of tokens that were " +
									"requested (minimumTokensRequested).\n" +
									"minimumTokensRequested: " + minimumTokens + "\n" +
									"containerMaximumTokens: " + containerMaximumTokens);
							}
							return earliestConsumption;
						}
						if (firstContainer == null)
							firstContainer = container;

						long containerMaximumTokens = CONTAINER_SECRETS.getMaximumTokens(container);
						if (containerMaximumTokens < minimumTokens)
							continue;
						ConsumptionResult consumptionResult = CONTAINER_SECRETS.tryConsume(container, minimumTokens,
							maximumTokens, nameOfMinimumTokens, requestedAt, consumedAt);
						if (consumptionResult.isSuccessful())
							return consumptionResult;
						if (earliestConsumption == null ||
							earliestConsumption.getAvailableAt().isAfter(consumptionResult.getAvailableAt()))
						{
							earliestConsumption = consumptionResult;
						}
					}
				}
			};
	private static final ConsumptionFunction CONSUME_FROM_ALL_POLICY =
		(minimumTokens, maximumTokens, nameOfMinimumTokens, requestedAt, consumedAt, abstractBucket) ->
		{
			ContainerList containerList = (ContainerList) abstractBucket;

			List<CloseableLock> locks = new ArrayList<>();
			try
			{
				// Prevent the list of descendents from changing
				List<AbstractContainer> descendants = CONTAINER_SECRETS.getDescendants(containerList);
				assertThat(r -> r.requireThat(descendants, "descendants").isNotEmpty());
				for (AbstractContainer descendant : descendants)
					locks.add(CONTAINER_SECRETS.getLock(descendant).readLock());

				for (AbstractContainer descendant : descendants)
				{
					// Prevent the number of tokens from changing
					if (descendant instanceof Bucket bucket)
					{
						for (Limit limit : bucket.getLimits())
							locks.add(limit.lock.writeLock());
					}
				}
				requireThat(minimumTokens, nameOfMinimumTokens).
					isLessThanOrEqualTo(containerList.getMaximumTokens(), "containerList.getMaximumTokens()");

				long tokensToConsume = maximumTokens;
				long tokensLeft = Long.MAX_VALUE;
				@SuppressWarnings("unchecked")
				List<AbstractContainer> children = (List<AbstractContainer>) (List<?>) containerList.getChildren();
				for (AbstractContainer child : children)
				{
					long availableTokens = CONTAINER_SECRETS.getAvailableTokens(child);
					tokensToConsume = Math.min(tokensToConsume, availableTokens);
					tokensLeft = Math.min(tokensLeft, availableTokens);
				}
				if (tokensToConsume < minimumTokens)
				{
					List<Limit> bottlenecks = new ArrayList<>();
					for (AbstractContainer child : children)
						bottlenecks.addAll(CONTAINER_SECRETS.getLimitsWithInsufficientTokens(child, minimumTokens));
					return new ConsumptionResult(containerList, minimumTokens, maximumTokens, 0,
						requestedAt, consumedAt, consumedAt, tokensLeft, bottlenecks);
				}

				for (AbstractContainer container : children)
				{
					// Consume an equal number of tokens across all containers, even if some have more available.
					ConsumptionResult consumptionResult = CONTAINER_SECRETS.tryConsume(container, tokensToConsume,
						tokensToConsume, nameOfMinimumTokens, requestedAt, consumedAt);
					long finalTokensToConsume = tokensToConsume;
					assertThat(r ->
					{
						r.requireThat(consumptionResult.isSuccessful(), "consumptionResult.isSuccessful()").isTrue();
						r.requireThat(consumptionResult.getTokensConsumed(),
							"consumptionResult.getTokensConsumed()").isEqualTo(finalTokensToConsume, "tokensToConsume");
					});
				}
				tokensLeft -= tokensToConsume;
				return new ConsumptionResult(containerList, minimumTokens, maximumTokens, tokensToConsume,
					requestedAt, consumedAt, consumedAt, tokensLeft, List.of());
			}
			finally
			{
				Collections.reverse(locks);
				for (CloseableLock lock : locks)
					lock.close();
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
		return new Builder();
	}

	private ConsumptionPolicy consumptionPolicy;
	private SelectionPolicy selectionPolicy;
	private final Logger log = LoggerFactory.getLogger(ContainerList.class);

	/**
	 * Creates a new ContainerList.
	 *
	 * @param listeners           the event listeners associated with this list
	 * @param children            the children in this list
	 * @param userData            the data associated with this list
	 * @param consumptionPolicy   indicates how tokens are consumed
	 * @param consumptionFunction indicates how tokens are consumed
	 * @param selectionPolicy     the {@link SelectionPolicy} used by the {@code consumptionPolicy}
	 *                            ({@code null} if none is used)
	 * @throws NullPointerException     if any of the arguments (aside from {@code userData}) and
	 *                                  {@code selectionPolicy} are null
	 * @throws IllegalArgumentException if {@code children} are empty
	 */
	private ContainerList(List<AbstractContainer> children, List<ContainerListener> listeners, Object userData,
	                      ConsumptionPolicy consumptionPolicy, ConsumptionFunction consumptionFunction,
	                      SelectionPolicy selectionPolicy)
	{
		super(children, listeners, userData, consumptionFunction);
		assertThat(r ->
		{
			r.requireThat(children, "children").isNotEmpty();
			r.requireThat(consumptionPolicy, "consumptionPolicy").isNotNull();
		});
		this.consumptionPolicy = consumptionPolicy;
		this.selectionPolicy = selectionPolicy;
	}

	@Override
	protected Logger getLogger()
	{
		return log;
	}

	@Override
	protected long getAvailableTokens()
	{
		long availableTokens = Long.MAX_VALUE;
		for (AbstractContainer child : children)
			availableTokens = Math.min(availableTokens, CONTAINER_SECRETS.getAvailableTokens(child));
		return availableTokens;
	}

	@Override
	protected List<Limit> getLimitsWithInsufficientTokens(long tokens)
	{
		List<Limit> result = new ArrayList<>();
		for (AbstractContainer child : children)
			result.addAll(CONTAINER_SECRETS.getLimitsWithInsufficientTokens(child, tokens));
		return result;
	}

	@Override
	protected long getMaximumTokens()
	{
		return consumptionPolicy.getMaximumTokens(this);
	}

	/**
	 * Returns the number of children in this list. If this list contains more than {@code Integer.MAX_VALUE}
	 * children, returns {@code Integer.MAX_VALUE}.
	 *
	 * @return the number of children in this list
	 */
	public int size()
	{
		return lock.optimisticReadLock(() -> children.size());
	}

	@Override
	public String toString()
	{
		return lock.optimisticReadLock(() ->
			new ToStringBuilder(ContainerList.class).
				add("consumptionPolicy", consumptionPolicy).
				add("children", children).
				add("userData", userData).
				toString());
	}

	/**
	 * Updates this list's configuration.
	 * <p>
	 * The {@code ContainerList} will be locked until {@link ConfigurationUpdater#close()} is invoked.
	 *
	 * @return the configuration updater
	 */
	@CheckReturnValue
	public ConfigurationUpdater updateConfiguration()
	{
		return new ConfigurationUpdater();
	}

	/**
	 * Builds a ContainerList.
	 */
	public static final class Builder
	{
		private final List<ContainerListener> listeners = new ArrayList<>();
		private final List<AbstractContainer> children = new ArrayList<>();
		private Object userData;
		private ConsumptionPolicy consumptionPolicy;
		private ConsumptionFunction consumptionFunction;
		private SelectionPolicy selectionPolicy;

		/**
		 * Creates a new builder.
		 * <p>
		 * By default, the list will delegate to the first child that has sufficient number of tokens available,
		 * using the round-robin scheduling policy.
		 */
		private Builder()
		{
			consumeFromOne(SelectionPolicy.ROUND_ROBIN);
		}

		/**
		 * Returns the consumption policy indicating how to consume tokens from children containers.
		 *
		 * @return the consumption policy
		 */
		public ConsumptionPolicy consumptionPolicy()
		{
			return consumptionPolicy;
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
			this.consumptionPolicy = ConsumptionPolicy.CONSUME_FROM_ONE;
			this.consumptionFunction = CONSUME_FROM_ONE_POLICY.apply(selectionPolicy.createSelector());
			this.selectionPolicy = selectionPolicy;
			return this;
		}

		/**
		 * Indicates that the list should consume tokens from all children simultaneously.
		 *
		 * @return this
		 */
		public Builder consumeFromAll()
		{
			this.consumptionPolicy = ConsumptionPolicy.CONSUME_FROM_ALL;
			this.consumptionFunction = CONSUME_FROM_ALL_POLICY;
			this.selectionPolicy = null;
			return this;
		}

		/**
		 * Returns the event listeners associated with this list.
		 *
		 * @return this
		 */
		public List<ContainerListener> listeners()
		{
			return listeners;
		}

		/**
		 * Adds an event listener to the list.
		 *
		 * @param listener a listener
		 * @return this
		 * @throws NullPointerException if {@code listener} is null
		 */
		public Builder addListener(ContainerListener listener)
		{
			requireThat(listener, "listener").isNotNull();
			listeners.add(listener);
			return this;
		}

		/**
		 * Returns the children containers.
		 *
		 * @return the children containers
		 */
		public List<Container> children()
		{
			return List.copyOf(children);
		}

		/**
		 * Adds a Bucket to this list.
		 *
		 * @param bucketBuilder builds the Bucket
		 * @return this
		 * @throws NullPointerException if {@code bucketBuilder} is null
		 */
		public Builder addBucket(Function<Bucket.Builder, Bucket> bucketBuilder)
		{
			requireThat(bucketBuilder, "bucketBuilder").isNotNull();
			Bucket child = bucketBuilder.apply(Bucket.builder());
			children.add(child);
			return this;
		}

		/**
		 * Adds a ContainerList to this list.
		 *
		 * @param listBuilder builds the ContainerList
		 * @return this
		 * @throws NullPointerException if {@code listBuilder} is null
		 */
		public Builder addContainerList(Function<ContainerList.Builder, ContainerList> listBuilder)
		{
			requireThat(listBuilder, "listBuilder").isNotNull();
			ContainerList child = listBuilder.apply(ContainerList.builder());
			children.add(child);
			return this;
		}

		/**
		 * Returns the user data associated with this list.
		 *
		 * @return the data associated with this list
		 */
		@CheckReturnValue
		public Object userData()
		{
			return userData;
		}

		/**
		 * Sets user data associated with this list.
		 *
		 * @param userData the data associated with this list
		 * @return this
		 */
		@CheckReturnValue
		public Builder userData(Object userData)
		{
			if (Objects.equals(userData, this.userData))
				return this;
			this.userData = userData;
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
			ContainerList containerList = new ContainerList(children, listeners, userData, consumptionPolicy,
				consumptionFunction, selectionPolicy);
			for (AbstractContainer child : children)
				CONTAINER_SECRETS.setParent(child, containerList);
			return containerList;
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder(Builder.class).
				add("consumptionPolicy", consumptionPolicy).
				add("selectionPolicy", selectionPolicy).
				add("children", children).
				add("userData", userData).
				toString();
		}
	}

	/**
	 * Updates this ContainerList's configuration.
	 * <p>
	 * <b>Thread-safety</b>: This class is <b>not</b> thread-safe.
	 */
	public final class ConfigurationUpdater implements AutoCloseable
	{
		private final CloseableLock writeLock = lock.writeLock();
		private boolean closed;
		private ConsumptionFunction consumptionFunction;
		private ConsumptionPolicy consumptionPolicy;
		private SelectionPolicy selectionPolicy;
		private final List<ContainerListener> listeners;
		private final List<AbstractContainer> children;
		private Object userData;
		private boolean wakeConsumers;
		private boolean changed;

		/**
		 * Creates a new configuration updater.
		 */
		private ConfigurationUpdater()
		{
			this.listeners = new ArrayList<>(ContainerList.this.listeners);
			this.children = new ArrayList<>(ContainerList.this.children);
			this.userData = ContainerList.this.userData;
			this.consumptionFunction = ContainerList.this.consumptionFunction;
			this.consumptionPolicy = ContainerList.this.consumptionPolicy;
			this.selectionPolicy = ContainerList.this.selectionPolicy;
		}

		/**
		 * Returns the consumption policy indicating how to consume tokens from children containers.
		 *
		 * @return the consumption policy
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConsumptionPolicy consumptionPolicy()
		{
			ensureOpen();
			return consumptionPolicy;
		}

		/**
		 * Indicates that the list should delegate to the first child that has sufficient number of tokens
		 * available.
		 *
		 * @param selectionPolicy determines the order in which buckets are evaluated
		 * @return this
		 * @throws NullPointerException  if {@code selectionPolicy} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater consumeFromOne(SelectionPolicy selectionPolicy)
		{
			requireThat(selectionPolicy, "selectionPolicy").isNotNull();
			ensureOpen();
			if (this.consumptionPolicy == ConsumptionPolicy.CONSUME_FROM_ONE &&
				this.selectionPolicy == selectionPolicy)
			{
				return this;
			}
			changed = true;
			this.consumptionPolicy = ConsumptionPolicy.CONSUME_FROM_ONE;
			this.consumptionFunction = CONSUME_FROM_ONE_POLICY.apply(selectionPolicy.createSelector());
			this.selectionPolicy = selectionPolicy;
			return this;
		}

		/**
		 * Indicates that the list should consume tokens from all children simultaneously.
		 *
		 * @return this
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater consumeFromAll()
		{
			ensureOpen();
			if (this.consumptionPolicy == ConsumptionPolicy.CONSUME_FROM_ALL)
				return this;
			changed = true;
			this.consumptionPolicy = ConsumptionPolicy.CONSUME_FROM_ALL;
			this.consumptionFunction = CONSUME_FROM_ALL_POLICY;
			this.selectionPolicy = null;
			return this;
		}

		/**
		 * Returns the children containers.
		 *
		 * @return the children containers
		 * @throws IllegalStateException if the updater is closed
		 */
		public List<Container> children()
		{
			ensureOpen();
			return List.copyOf(children);
		}

		/**
		 * Adds a Bucket to this list.
		 *
		 * @param bucketBuilder builds the Bucket
		 * @return this
		 * @throws NullPointerException  if {@code bucketBuilder} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater addBucket(Function<Bucket.Builder, Bucket> bucketBuilder)
		{
			requireThat(bucketBuilder, "bucketBuilder").isNotNull();
			ensureOpen();
			Bucket child = bucketBuilder.apply(Bucket.builder());
			children.add(child);
			changed = true;
			if (consumptionPolicy == ConsumptionPolicy.CONSUME_FROM_ONE)
			{
				// Adding a new bucket may allow consumers to consume tokens earlier than originally anticipated
				wakeConsumers = true;
			}
			return this;
		}

		/**
		 * Adds a ContainerList to this list.
		 *
		 * @param listBuilder builds the ContainerList
		 * @return this
		 * @throws NullPointerException  if {@code listBuilder} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater addContainerList(Function<ContainerList.Builder, ContainerList> listBuilder)
		{
			requireThat(listBuilder, "listBuilder").isNotNull();
			ensureOpen();
			ContainerList child = listBuilder.apply(ContainerList.builder());
			children.add(child);
			changed = true;
			if (consumptionPolicy == ConsumptionPolicy.CONSUME_FROM_ONE)
			{
				// Adding a new bucket may allow consumers to consume tokens earlier than originally anticipated
				wakeConsumers = true;
			}
			return this;
		}

		/**
		 * Removes a child from this list.
		 *
		 * @param child a child
		 * @return this
		 * @throws NullPointerException  if {@code child} is null
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater remove(Container child)
		{
			requireThat(child, "child").isNotNull();
			ensureOpen();
			if (children.remove((AbstractContainer) child))
				changed = true;
			return this;
		}

		/**
		 * Removes all children from this list.
		 *
		 * @return this
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater clear()
		{
			ensureOpen();
			if (children.isEmpty())
				return this;
			changed = true;
			children.clear();
			return this;
		}

		/**
		 * Returns the user data associated with this list.
		 *
		 * @return the data associated with this list
		 * @throws IllegalStateException if the updater is closed
		 */
		@CheckReturnValue
		public Object userData()
		{
			ensureOpen();
			return userData;
		}

		/**
		 * Sets user data associated with this list.
		 *
		 * @param userData the data associated with this list
		 * @return this
		 * @throws IllegalStateException if the updater is closed
		 */
		public ConfigurationUpdater userData(Object userData)
		{
			ensureOpen();
			if (Objects.equals(userData, this.userData))
				return this;
			changed = true;
			this.userData = userData;
			return this;
		}

		/**
		 * @throws IllegalStateException if the updater is closed
		 */
		private void ensureOpen()
		{
			if (closed)
				throw new IllegalStateException("ConfigurationUpdater is closed");
		}

		/**
		 * Updates this ContainerList's configuration.
		 *
		 * @throws IllegalArgumentException if {@code children} is empty
		 */
		@Override
		public void close()
		{
			if (closed)
				return;
			closed = true;
			try
			{
				// There is no way to fix a ConfigurationUpdater once try-with-resources exits, so the updater is
				// closed and the write-lock released even if an exception is thrown.
				requireThat(children, "children").isNotEmpty();

				if (!changed)
					return;
				ContainerList.this.children = List.copyOf(children);
				ContainerList.this.listeners = List.copyOf(listeners);
				ContainerList.this.consumptionPolicy = consumptionPolicy;
				ContainerList.this.consumptionFunction = consumptionFunction;
				ContainerList.this.selectionPolicy = selectionPolicy;
				ContainerList.this.userData = userData;
				for (AbstractContainer child : children)
					CONTAINER_SECRETS.setParent(child, ContainerList.this);
			}
			finally
			{
				writeLock.close();
			}
			if (wakeConsumers)
			{
				try (CloseableLock ignored = conditionLock.writeLock())
				{
					tokensUpdated.signalAll();
				}
			}
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder(ConfigurationUpdater.class).
				add("consumptionPolicy", consumptionPolicy).
				add("selectionPolicy", selectionPolicy).
				add("children", children).
				add("userData", userData).
				add("changed", changed).
				toString();
		}
	}
}