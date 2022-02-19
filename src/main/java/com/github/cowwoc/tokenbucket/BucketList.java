package com.github.cowwoc.tokenbucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A list of buckets.
 *
 * <b>Thread safety</b>: This class is thread-safe.
 */
public final class BucketList extends AbstractBucket
	implements List<Bucket>
{
	private final Object mutex = new Object();
	private final SchedulingPolicy schedulingPolicy;
	private final List<Bucket> buckets = new ArrayList<>();
	private final Logger log = LoggerFactory.getLogger(BucketList.class);

	/**
	 * Creates a new BucketList.
	 *
	 * @param schedulingPolicy indicates how to select the next bucket to perform a task
	 * @throws NullPointerException if {@code schedulingPolicy} is null
	 */
	public BucketList(SchedulingPolicy schedulingPolicy)
	{
		requireThat(schedulingPolicy, "schedulingPolicy").isNotNull();
		this.schedulingPolicy = schedulingPolicy;
	}

	@Override
	public Object getMutex()
	{
		return mutex;
	}

	@Override
	protected Logger getLogger()
	{
		return log;
	}

	/**
	 * Adds a bucket to the list.
	 * <p>
	 * Buckets may not be modified while they are in the list. Doing so will result in undefined behavior.
	 * To modify a bucket, remove it from the list, modify it, and add it back.
	 *
	 * @param bucket the bucket to add
	 * @return true if this list changed as a result of the call
	 * @throws NullPointerException if {@code bucket} is null
	 */
	public boolean add(Bucket bucket)
	{
		requireThat(bucket, "bucket").isNotNull();
		synchronized (mutex)
		{
			// Limits have changed
			mutex.notifyAll();
			return this.buckets.add(bucket);
		}
	}

	/**
	 * Removes a bucket from the list.
	 *
	 * @param bucket the bucket to remove
	 * @return true if this list changed as a result of the call
	 * @throws NullPointerException if {@code bucket} is null
	 */
	public boolean remove(Bucket bucket)
	{
		requireThat(bucket, "bucket").isNotNull();
		synchronized (mutex)
		{
			// Limits have changed
			mutex.notifyAll();
			return this.buckets.remove(bucket);
		}
	}

	@Override
	protected ConsumptionResult tryConsumeRange(long minimumTokensRequested,
	                                            long maximumTokensRequested, Instant requestedAt)
	{
		Bucket firstBucket = null;
		ConsumptionResult earliestConsumption = null;
		synchronized (mutex)
		{
			while (true)
			{
				Bucket bucket = schedulingPolicy.nextBucket(buckets);
				ConsumptionResult consumptionResult = bucket.tryConsumeRange(minimumTokensRequested,
					maximumTokensRequested, requestedAt);
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
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof BucketList other))
			return false;
		return other.schedulingPolicy.equals(schedulingPolicy) &&
			other.buckets.equals(buckets);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(schedulingPolicy, buckets);
	}

	@Override
	public String toString()
	{
		return "buckets=" + buckets + ", schedulingPolicy: " + schedulingPolicy;
	}

	// Delegate List, Collection, Iterable methods to buckets

	@Override
	public int size()
	{
		return buckets.size();
	}

	@Override
	public boolean isEmpty()
	{
		return buckets.isEmpty();
	}

	@Override
	public boolean contains(Object o)
	{
		return buckets.contains(o);
	}

	@Override
	public Iterator<Bucket> iterator()
	{
		return buckets.iterator();
	}

	@Override
	public Object[] toArray()
	{
		return buckets.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a)
	{
		return buckets.toArray(a);
	}

	@Override
	public boolean remove(Object o)
	{
		return buckets.remove(o);
	}

	@Override
	public boolean containsAll(Collection<?> c)
	{
		return buckets.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends Bucket> c)
	{
		return buckets.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends Bucket> c)
	{
		return buckets.addAll(index, c);
	}

	@Override
	public boolean removeAll(Collection<?> c)
	{
		return buckets.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c)
	{
		return buckets.retainAll(c);
	}

	@Override
	public void replaceAll(UnaryOperator<Bucket> operator)
	{
		buckets.replaceAll(operator);
	}

	@Override
	public void sort(Comparator<? super Bucket> c)
	{
		buckets.sort(c);
	}

	@Override
	public void clear()
	{
		buckets.clear();
	}

	@Override
	public Bucket get(int index)
	{
		return buckets.get(index);
	}

	@Override
	public Bucket set(int index, Bucket element)
	{
		return buckets.set(index, element);
	}

	@Override
	public void add(int index, Bucket element)
	{
		buckets.add(index, element);
	}

	@Override
	public Bucket remove(int index)
	{
		return buckets.remove(index);
	}

	@Override
	public int indexOf(Object o)
	{
		return buckets.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o)
	{
		return buckets.lastIndexOf(o);
	}

	@Override
	public ListIterator<Bucket> listIterator()
	{
		return buckets.listIterator();
	}

	@Override
	public ListIterator<Bucket> listIterator(int index)
	{
		return buckets.listIterator(index);
	}

	@Override
	public List<Bucket> subList(int fromIndex, int toIndex)
	{
		return buckets.subList(fromIndex, toIndex);
	}

	@Override
	public Spliterator<Bucket> spliterator()
	{
		return buckets.spliterator();
	}

	@Override
	public <T> T[] toArray(IntFunction<T[]> generator)
	{
		return buckets.toArray(generator);
	}

	@Override
	public boolean removeIf(Predicate<? super Bucket> filter)
	{
		return buckets.removeIf(filter);
	}

	@Override
	public Stream<Bucket> stream()
	{
		return buckets.stream();
	}

	@Override
	public Stream<Bucket> parallelStream()
	{
		return buckets.parallelStream();
	}

	@Override
	public void forEach(Consumer<? super Bucket> action)
	{
		buckets.forEach(action);
	}
}