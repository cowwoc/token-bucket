package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.tokenbucket.internal.WrappingException.CallableWithoutReturnValue;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.StampedLock;

/**
 * A reentrant implementation of a StampedLock that can be used with try-with-resources.
 */
public final class ReentrantStampedLock
{
	// Excellent overview of StampedLock:
	// https://www.javaspecialists.eu/talks/pdfs/2014%20JavaLand%20in%20Germany%20-%20%22Java%208%20From%20Smile%20To%20Tears%20-%20Emotional%20StampedLock%22%20by%20Heinz%20Kabutz.pdf
	private final StampedLock lock = new StampedLock();
	/**
	 * The stamp associated with the current thread. {@code null} if none.
	 */
	private final ThreadLocal<Long> stamp = new ThreadLocal<>();

	/**
	 * Creates a new lock.
	 */
	public ReentrantStampedLock()
	{
	}

	/**
	 * Runs a task while holding an optimistic read lock. {@code task} is guaranteed to be invoked
	 * <i>at least</i> once, but must be safe to invoke multiple times as well. The return value must
	 * correspond to a local copy of the fields being read as there is no guarantee that state won't change
	 * between the time the lock is released and the time that the value is returned.
	 *
	 * @param <V>  the type of value returned by the task
	 * @param task the task to run while holding the lock
	 * @return the value returned by the task
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	public <V> V optimisticReadLock(Callable<V> task)
	{
		Long existingStamp = this.stamp.get();
		if (existingStamp != null)
		{
			if (StampedLock.isOptimisticReadStamp(existingStamp))
				return runWithOptimisticReadLock(task, existingStamp);
			if (StampedLock.isLockStamp(existingStamp))
				return runTaskWithCorrectLockType(task);
		}
		long stamp = lock.tryOptimisticRead();
		try
		{
			this.stamp.set(stamp);
			return runWithOptimisticReadLock(task, stamp);
		}
		finally
		{
			this.stamp.set(existingStamp);
			// There is nothing to unlock for optimistic reads
		}
	}

	/**
	 * Runs a task while holding a read lock.
	 *
	 * @param task the task to run while holding the lock
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	public void optimisticReadLock(CallableWithoutReturnValue task)
	{
		optimisticReadLock(() ->
		{
			task.run();
			return null;
		});
	}

	/**
	 * Runs a task while holding an optimistic read lock. {@code task} is guaranteed to be invoked
	 * <i>at least</i> once, but must be safe to invoke multiple times as well. The return value must
	 * correspond to a local copy of the fields being read as there is no guarantee that state won't change
	 * between the time the lock is released and the time that the value is returned.
	 *
	 * @param <V>   the type of value returned by the task
	 * @param task  the task to run while holding the lock
	 * @param stamp the optimistic read lock to use
	 * @return the value returned by the task
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	private <V> V runWithOptimisticReadLock(Callable<V> task, long stamp)
	{
		if (stamp != 0)
		{
			try
			{
				V result = task.call();
				if (lock.validate(stamp))
					return result;
			}
			catch (RuntimeException e)
			{
				throw e;
			}
			catch (Exception e)
			{
				throw WrappingException.wrap(e);
			}
		}
		return readLock(task);
	}

	/**
	 * Acquires a read lock.
	 *
	 * @return the lock as a resource
	 */
	public CloseableLock readLock()
	{
		Long existingStamp = this.stamp.get();
		if (existingStamp != null && StampedLock.isLockStamp(existingStamp))
			return ReentrantStampedLock::doNotUnlock;
		long stamp = lock.readLock();
		this.stamp.set(stamp);
		return () ->
		{
			this.stamp.set(existingStamp);
			lock.unlockRead(stamp);
		};
	}

	/**
	 * Runs a task while holding a read lock.
	 *
	 * @param <V>  the type of value returned by the task
	 * @param task the task to run while holding the lock
	 * @return the value returned by the task
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	public <V> V readLock(Callable<V> task)
	{
		Long existingStamp = this.stamp.get();
		if (existingStamp != null && StampedLock.isLockStamp(existingStamp))
			return runTaskWithCorrectLockType(task);
		long stamp = lock.readLock();
		try
		{
			this.stamp.set(stamp);
			return runTaskWithCorrectLockType(task);
		}
		finally
		{
			this.stamp.set(existingStamp);
			lock.unlockRead(stamp);
		}
	}

	/**
	 * Runs a task while holding a read lock.
	 *
	 * @param task the task to run while holding the lock
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	public void readLock(CallableWithoutReturnValue task)
	{
		readLock(() ->
		{
			task.run();
			return null;
		});
	}

	/**
	 * Acquires a write lock.
	 *
	 * @return the lock as a resource
	 */
	public CloseableLock writeLock()
	{
		Long existingStamp = this.stamp.get();
		if (existingStamp != null && StampedLock.isWriteLockStamp(existingStamp))
			return ReentrantStampedLock::doNotUnlock;
		long stamp = lock.writeLock();
		this.stamp.set(stamp);
		return () ->
		{
			this.stamp.set(existingStamp);
			lock.unlockWrite(stamp);
		};
	}

	/**
	 * Runs a task while holding a write lock.
	 *
	 * @param <V>  the type of value returned by the task
	 * @param task the task to run while holding the lock
	 * @return the value returned by the task
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	public <V> V writeLock(Callable<V> task)
	{
		Long existingStamp = this.stamp.get();
		if (existingStamp != null && StampedLock.isWriteLockStamp(existingStamp))
			return runTaskWithCorrectLockType(task);
		long stamp = lock.writeLock();
		try
		{
			this.stamp.set(stamp);
			return runTaskWithCorrectLockType(task);
		}
		finally
		{
			this.stamp.set(existingStamp);
			lock.unlockWrite(stamp);
		}
	}

	/**
	 * Runs a task while holding a write lock.
	 *
	 * @param task the task to run while holding the lock
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	public void writeLock(CallableWithoutReturnValue task)
	{
		writeLock(() ->
		{
			task.run();
			return null;
		});
	}

	/**
	 * Runs a task while holding a lock that has been verified to be a read lock.
	 *
	 * @param <V>  the type of value returned by the task
	 * @param task the task to run while holding the lock
	 * @return the value returned by the task
	 * @throws NullPointerException if {@code task} is null
	 * @throws WrappingException    if {@code task} throws a checked exception
	 */
	private <V> V runTaskWithCorrectLockType(Callable<V> task)
	{
		try
		{
			return task.call();
		}
		catch (RuntimeException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw WrappingException.wrap(e);
		}
	}

	private static void doNotUnlock()
	{
	}
}