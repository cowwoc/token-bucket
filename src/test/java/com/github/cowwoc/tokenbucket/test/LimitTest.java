package com.github.cowwoc.tokenbucket.test;

import com.github.cowwoc.tokenbucket.Bucket;
import com.github.cowwoc.tokenbucket.Limit;
import org.testng.annotations.Test;

import java.time.Duration;

import static com.github.cowwoc.requirements.DefaultRequirements.requireThat;

public final class LimitTest
{
	@Test
	public void updateConfiguration()
	{
		Bucket bucket = new Bucket();
		Limit limit = bucket.addLimit(1, Duration.ofSeconds(1)).apply();
		requireThat(limit.getTokensPerPeriod(), "tokensPerPeriod").isEqualTo(1L);
		limit.updateConfiguration().tokensPerPeriod(10).apply();
		requireThat(limit.getTokensPerPeriod(), "tokensPerPeriod").isEqualTo(10L);
	}
}
