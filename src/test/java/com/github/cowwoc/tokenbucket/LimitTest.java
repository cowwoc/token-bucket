package com.github.cowwoc.tokenbucket;

import com.github.cowwoc.tokenbucket.Limit.ConfigurationUpdater;
import org.testng.annotations.Test;

import java.util.List;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class LimitTest
{
	@Test
	public void updateConfiguration()
	{
		Bucket bucket = Bucket.builder().
			addLimit(limit -> limit.userData("limit").build()).
			build();
		List<Limit> limits = bucket.getLimits();
		requireThat(limits, "limits").size().isEqualTo(1);
		Limit limit = limits.iterator().next();
		requireThat(limit.getTokensPerPeriod(), "limit.getTokensPerPeriod()").isEqualTo(1L);
		requireThat(limit.getUserData(), "limit.getUserData()").isEqualTo("limit");

		try (ConfigurationUpdater update = limit.updateConfiguration())
		{
			update.tokensPerPeriod(5);
		}
		requireThat(limit.getTokensPerPeriod(), "limit.getTokensPerPeriod()").isEqualTo(5L);
	}
}