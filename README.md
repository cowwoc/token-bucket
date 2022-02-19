[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.cowwoc.token-bucket/java/badge.svg)](https://search.maven.org/search?q=g:com.github.cowwoc.token-bucket)
[![build-status](../../workflows/Build/badge.svg)](../../actions?query=workflow%3ABuild)

# <img src="wiki/bucket.svg" width=64 height=64 alt="checklist"> Token-Bucket

[![API](https://img.shields.io/badge/api_docs-5B45D5.svg)](https://cowwoc.github.io/token-bucket/2.0/docs/api/)
[![Changelog](https://img.shields.io/badge/changelog-A345D5.svg)](wiki/Changelog.md)

A Java implementation of the [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket).

# Download

You can download this library from https://search.maven.org/search?q=g:com.github.cowwoc.token-bucket or using
the following Maven dependency:

```
<dependency>
  <groupId>com.github.cowwoc.token-bucket</groupId>
  <artifactId>token-bucket</artifactId>
  <version>2.0</version>
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


// Polls a server with a limit of 5 requests per second and a maximum burst of 120 requests.
while (true)
{
  bucket.consume();
  client.pollServer();
}
```

# License

* Code licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
* Icons from www.svgrepo.com licensed under the CC0 License 