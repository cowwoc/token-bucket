package com.github.cowwoc.tokenbucket.internal;

import java.util.List;

/**
 * Selects a container.
 */
public interface ContainerSelector
{
	/**
	 * Selects the next container to perform a task.
	 *
	 * @param containers a list of container
	 * @return the next container to perform a task
	 * @throws NullPointerException     if {@code containers} is null
	 * @throws IllegalArgumentException if {@code containers} is empty
	 */
	AbstractContainer nextContainer(List<AbstractContainer> containers);
}