[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.cowwoc.token-bucket/token-bucket/badge.svg)](https://search.maven.org/search?q=g:com.github.cowwoc.token-bucket)
[![build-status](../../workflows/Build/badge.svg)](../../actions?query=workflow%3ABuild)

# <img src="wiki/bucket.svg" width=64 height=64 alt="checklist"> Token-Bucket

[![API](https://img.shields.io/badge/api_docs-5B45D5.svg)](https://cowwoc.github.io/token-bucket/4.1/docs/api/)
[![Changelog](https://img.shields.io/badge/changelog-A345D5.svg)](wiki/Changelog.md)

A Java implementation of the [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket).

# Download

You can download this library from https://search.maven.org/search?q=g:com.github.cowwoc.token-bucket or using
the following Maven dependency:

```
<dependency>
  <groupId>com.github.cowwoc.token-bucket</groupId>
  <artifactId>token-bucket</artifactId>
  <version>4.1</version>
</dependency>
```

# Usage

```
// Allow 60 requests per minute, with a maximum burst of 120 requests.
Bucket server1 = Bucket.builder().
  addLimit(limit -> limit.
    tokensPerPeriod(60).
    period(Duration.ofMinute(1)).
    maxTokens(120).
    build()).
  build();

// Allow 60 requests per minute, with a maximum burst of 10 requests per second.
Bucket server2 = Bucket.builder().
  addLimit(limit -> limit.
    tokensPerPeriod(60).
    period(Duration.ofMinute(1)).
    build()).
  addLimit(limit -> limit.
    tokensPerPeriod(10).
    period(Duration.ofSecond(1)).
    build()).
  build();

// Choose a server and send requests
Bucket bucket = server1;

while (true)
{
  bucket.consume();
  client.pollServer();
}
```

# License

* Code licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
* Icons from www.svgrepo.com licensed under the CC0 License 