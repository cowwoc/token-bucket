Minor updates involving cosmetic changes have been omitted from this list.
See https://github.com/cowwoc/token-bucket/commits/master for a full list.

## Version 3.0 - ?

* Breaking changes:
    * Limits
        * Renamed tokensAvailable to availableTokens.
        * Renamed maxTokens to maximumTokens.
        * Renamed minimumToRefill to minimumRefill.
    * Container
        * Renamed consumeRange() to consume().
        * Disallow consumption of zero tokens.
    * ConsumptionResult
        * Renamed getTokensAvailableAt() to getAvailableAt().
        * Renamed getTokensAvailableIn() to getAvailableIn().
* New features
    * Ability to look up existing values from Builder, ConfigurationUpdater classes.
    * Ability to navigate from a Limit to its Bucket and a Container to its parent.
* Improvements:
    * Don't wake up consumers unless the number of tokens is positive.
    * Allow negative Limit.initialTokens, availableTokens.
* Bugfixes:
    * ContainerList.updateConfiguration() was not updating the consumption policy.

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