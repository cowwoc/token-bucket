Minor updates involving cosmetic changes have been omitted from this list.
See https://github.com/cowwoc/token-bucket/commits/master for a full list.

## Version 2.0 - 2022/02/21

* Breaking changes:
    * Added `Limit.builder()`, `Bucket.builder()`, `ContainerList.builder()`.
    * `try/consume()` returns ConsumptionResult instead of a boolean.
* New features:
    * Ability to consume tokens from a container that contains one or more buckets.
    * Ability to consume a variable number of tokens using `try/consumeRange(minimumTokens, maximumTokens)`.
* Bugfix: `maxTokens` may be equal to `tokensPerPeriod` or `initialTokens`.

## Version 1.0 - 2020/02/12

* Initial release