package com.github.cowwoc.tokenbucket.internal;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.StringJoiner;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

/**
 * Standardizes to format of toString() return values.
 */
public final class ToStringBuilder
{
	private final Class<?> aClass;
	private final List<Entry<String, String>> properties = new ArrayList<>();

	/**
	 * Creates a new builder.
	 *
	 * @param aClass the type of object being processed
	 * @throws NullPointerException if {@code aClass} is null
	 */
	public ToStringBuilder(Class<?> aClass)
	{
		requireThat(aClass, "aClass").isNotNull();
		this.aClass = aClass;
	}

	/**
	 * Creates a new builder without an object type.
	 */
	public ToStringBuilder()
	{
		this.aClass = null;
	}

	/**
	 * Adds a property.
	 *
	 * @param name  the name of the property
	 * @param value the value of the property
	 * @return this
	 * @throws NullPointerException     if {@code name} is null
	 * @throws IllegalArgumentException if {@code name} is blank
	 */
	public ToStringBuilder add(String name, Object value)
	{
		if (value instanceof List<?> list)
			return add(name, list);
		requireThat(name, "name").isNotBlank();
		properties.add(new SimpleImmutableEntry<>(name, String.valueOf(value)));
		return this;
	}

	/**
	 * Adds a property.
	 *
	 * @param name the name of the property
	 * @param list the value of the property
	 * @return this
	 * @throws NullPointerException     if {@code name} is null
	 * @throws IllegalArgumentException if {@code name} is blank
	 */
	public ToStringBuilder add(String name, List<?> list)
	{
		requireThat(name, "name").isNotBlank();
		StringJoiner joiner = new StringJoiner(", ", "[", "]");
		for (Object anObject : list)
			joiner.add(anObject.toString());
		add(name, joiner);
		return this;
	}

	/**
	 * Adds a property.
	 *
	 * @param name the name of the property
	 * @param map  the value of the property
	 * @return this
	 * @throws NullPointerException     if {@code name} is null
	 * @throws IllegalArgumentException if {@code name} is blank
	 */
	public ToStringBuilder add(String name, Map<?, ?> map)
	{
		requireThat(name, "name").isNotBlank();
		ToStringBuilder entries = new ToStringBuilder();
		for (Entry<?, ?> entry : map.entrySet())
			entries.add(entry.getKey().toString(), entry.getValue());
		add(name, entries);
		return this;
	}

	/**
	 * @param text      the {@code String} to align
	 * @param minLength the minimum length of {@code text}
	 * @return {@code text} padded on the right with spaces until its length is greater than or equal to
	 * {@code minLength}
	 */
	private static String alignLeft(String text, int minLength)
	{
		int actualLength = text.length();
		if (actualLength > minLength)
			return text;
		return text + " ".repeat(minLength - actualLength);
	}

	/**
	 * Returns the String representation of this {@code ToStringBuilder}.
	 *
	 * @return the String representation of this {@code ToStringBuilder}
	 */
	public String toString()
	{
		int maxKeyLength = 0;
		for (Entry<String, String> entry : properties)
		{
			String key = entry.getKey();
			if (key.isBlank())
				continue;
			int length = key.length();
			if (length > maxKeyLength)
				maxKeyLength = length;
		}

		StringJoiner output = new StringJoiner(",\n");
		StringBuilder line = new StringBuilder();
		for (Entry<String, String> entry : properties)
		{
			line.delete(0, line.length());
			String key = entry.getKey();
			if (!key.isBlank())
				line.append(alignLeft(key, maxKeyLength)).append(": ");
			line.append(entry.getValue());
			output.add(line.toString());
		}
		String name;
		if (aClass != null)
		{
			Class<?> currentClass = aClass;
			Stack<String> names = new Stack<>();
			while (currentClass != null)
			{
				names.add(currentClass.getSimpleName());
				currentClass = currentClass.getEnclosingClass();
			}
			StringJoiner joiner = new StringJoiner(".");
			for (String simpleName : names)
				joiner.add(simpleName);
			name = joiner.toString();
		}
		else
			name = "";
		return name + "\n" +
			"{\n" +
			"\t" + output.toString().replaceAll("\n", "\n\t") + "\n" +
			"}";
	}
}