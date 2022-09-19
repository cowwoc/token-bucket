package com.github.cowwoc.tokenbucket.internal;

import com.github.cowwoc.requirements.DefaultRequirements;

import java.io.Serial;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * A runtime exception dedicated to wrapping checked exceptions.
 */
public class WrappingException extends RuntimeException
{
	@Serial
	private static final long serialVersionUID = 0L;

	/**
	 * Wraps an exception.
	 *
	 * @param message the detail message
	 * @param cause   the exception to wrap
	 * @throws NullPointerException if any of the arguments are null
	 */
	public WrappingException(String message, Throwable cause)
	{
		super(message, cause);
		requireThat(message, "message").isNotNull();
		requireThat(cause, "cause").isNotNull();
	}

	/**
	 * Wraps an exception.
	 *
	 * @param cause the exception to wrap
	 * @throws NullPointerException if {@code cause} is null
	 */
	private WrappingException(Throwable cause)
	{
		super(cause);
		requireThat(cause, "cause").isNotNull();
	}

	/**
	 * Wraps any checked exceptions thrown by a callable.
	 *
	 * @param callable the task to execute
	 * @param <V>      the type of value returned by {@code callable}
	 * @return the value returned by {@code callable}
	 * @throws NullPointerException if {@code callable} is null
	 */
	public static <V> V wrap(Callable<V> callable)
	{
		try
		{
			return callable.call();
		}
		catch (Exception e)
		{
			throw WrappingException.wrap(e);
		}
	}

	/**
	 * Wraps any checked exceptions thrown by a task.
	 *
	 * @param task the task to execute
	 * @throws NullPointerException if {@code task} is null
	 */
	public static void wrap(CallableWithoutReturnValue task)
	{
		wrap(() ->
		{
			task.run();
			return null;
		});
	}

	/**
	 * Wraps an exception, unless it is a {@code RuntimeException}.
	 *
	 * @param t the exception to wrap
	 * @return the updated exception
	 * @throws NullPointerException if {@code t} is null
	 */
	public static WrappingException wrap(Throwable t)
	{
		if (t instanceof RuntimeException re)
			throw re;
		if (t instanceof ExecutionException ee)
			return wrap(ee.getCause());
		return new WrappingException(DefaultRequirements.getContextMessage(null), t);
	}

	/**
	 * A {@link Callable} without a return value. {@link Runnable} cannot be used because it does not throw
	 * checked exceptions.
	 */
	@FunctionalInterface
	public interface CallableWithoutReturnValue
	{
		/**
		 * Runs the task.
		 *
		 * @throws Exception if unable to compute a result
		 */
		void run() throws Exception;
	}
}