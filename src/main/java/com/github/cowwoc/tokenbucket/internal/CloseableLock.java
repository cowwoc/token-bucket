package com.github.cowwoc.tokenbucket.internal;

/**
 * Let the compiler know that releasing a lock does not throw any exceptions.
 */
public interface CloseableLock extends AutoCloseable
{
	@Override
	void close();
}