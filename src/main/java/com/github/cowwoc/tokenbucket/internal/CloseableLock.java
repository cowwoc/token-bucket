package com.github.cowwoc.tokenbucket.internal;

/**
 * Informs the compilers that releasing a lock does not throw any exceptions.
 */
public interface CloseableLock extends AutoCloseable
{
	@Override
	void close();
}