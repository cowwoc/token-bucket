package com.github.cowwoc.tokenbucket.internal;

/**
 * A mechanism for calling package-private methods in another package without using reflection.
 *
 * @see <a href="https://stackoverflow.com/a/46723089/14731">https://stackoverflow.com/a/46723089/14731</a>
 */
public final class SharedSecrets
{
	/**
	 * The singleton instance.
	 */
	public static final SharedSecrets INSTANCE = new SharedSecrets();
	public ContainerSecrets containerSecrets;

	/**
	 * Prevent construction.
	 */
	private SharedSecrets()
	{
	}
}