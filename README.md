[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.cowwoc.token-bucket/java/badge.svg)](https://search.maven.org/search?q=g:com.github.cowwoc.token-bucket)
[![build-status](../../workflows/Build/badge.svg)](../../actions?query=workflow%3ABuild)

This is a Java implementation of the [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket).

# Download

You can download this library from https://search.maven.org/search?q=g:com.github.cowwoc.token-bucket or using the following Maven dependency:
```
<dependency>
  <groupId>com.github.cowwoc.token-bucket</groupId>
  <artifactId>token-bucket</artifactId>
  <version>1.0</version>
</dependency>
```

# Usage

```
Bucket bucket = new Bucket();

int tokensPerPeriod = 5;
Duration period = Duration.ofSeconds(1);
long initialTokens = 0;
long maxTokens = 120;
bucket.addLimit(new Limit(tokensPerPeriod, period, initialTokens, maxTokens));


// Polls a server with a limit of 5 requests per second and a maximum burst of 120 requests at a time.
while (true)
{
  bucket.consume(1);
  client.pollServer();
}
```

# License

* Code licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
