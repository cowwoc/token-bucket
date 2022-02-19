Minor updates involving cosmetic changes have been omitted from this list.
See https://github.com/cowwoc/token-bucket/commits/master for a full list.

## Version 2.0 - 2022/02/19

* Breaking changes:
    * `try/consume()` returns ConsumptionResult instead of boolean.
* New features:
    * Ability to `try/consume()` tokens from the first available bucket out of a list of buckets.
    * Ability to consume a variable number of tokens using `try/onsumeRange(minimumTokens, maximumTokens)`.
* Bugfix: `maxTokens` may be equal to `tokensPerPeriod` or `initialTokens`.

## Version 1.0 - 2020/02/12

* Initial release