package com.github.cowwoc.tokenbucket.internal;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Condition helper functions.
 */
public final class Conditions
{
	/**
	 * Prevent construction.
	 */
	private Conditions()
	{
	}

	/**
	 * Waits for a condition to get signalled or a timeout to occur.
	 *
	 * @param condition the condition
	 * @param duration  the duration to sleep
	 * @return false if the waiting time detectably elapsed before return from the method, else true
	 * @throws InterruptedException if the current thread is interrupted (and interruption of thread
	 *                              suspension is supported)
	 */
	public static boolean await(Condition condition, Duration duration) throws InterruptedException
	{
		try
		{
			long nanosLeft = duration.toNanos();
			while (nanosLeft > 0)
				nanosLeft = condition.awaitNanos(nanosLeft);
			return true;
		}
		catch (ArithmeticException e)
		{
			try
			{
				return condition.await(duration.toMillis(), TimeUnit.MILLISECONDS);
			}
			catch (ArithmeticException e2)
			{
				long seconds = duration.toSeconds();
				if (duration.getNano() > 0)
					++seconds;
				return condition.await(seconds, TimeUnit.SECONDS);
			}
		}
	}
}