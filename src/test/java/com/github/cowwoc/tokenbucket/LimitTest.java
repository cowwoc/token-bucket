package com.github.cowwoc.tokenbucket;

import org.testng.annotations.Test;

import java.util.Set;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class LimitTest
{
	@Test
	public void updateConfiguration()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit -> limit.userData("limit").build()).
			build();
		Set<Limit> limits = bucket.getLimits();
		requireThat(limits, "limits").size().isEqualTo(1);
		requireThat(limits.iterator().next().getUserData(), "limit.getUserData()").isEqualTo("limit");
	}
}
