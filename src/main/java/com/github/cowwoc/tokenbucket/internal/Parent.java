package com.github.cowwoc.tokenbucket.internal;

/**
 * A container of other containers.
 */
public interface Parent
{
	/**
	 * Updates a child.
	 *
	 * @param child  the child
	 * @param update updates the child
	 * @throws NullPointerException if any of the arguments are null
	 */
	void updateChild(Object child, Runnable update);
}
