package com.github.cowwoc.tokenbucket.internal;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Enables the use of try-with-resources with ReadWriteLock.
 */
public final class ReadWriteLockAsResource
{
	private final ReadWriteLock lock;

	/**
	 * Creates a new lock.
	 */
	public ReadWriteLockAsResource()
	{
		this.lock = new ReentrantReadWriteLock();
	}

	/**
	 * Acquires a read lock.
	 *
	 * @return the lock as a resource
	 */
	public CloseableLock readLock()
	{
		Lock readLock = lock.readLock();
		readLock.lock();
		return readLock::unlock;
	}

	/**
	 * Acquires a write lock.
	 *
	 * @return the lock as a resource
	 */
	public CloseableLock writeLock()
	{
		Lock writeLock = lock.writeLock();
		writeLock.lock();
		return writeLock::unlock;
	}

	/**
	 * Returns a new condition.
	 *
	 * @return a new condition
	 */
	public Condition newCondition()
	{
		return lock.writeLock().newCondition();
	}
}