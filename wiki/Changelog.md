Minor or cosmetic changes have been omitted from this list.
See https://github.com/cowwoc/token-bucket/commits/master for a full list.

## Version 6.0 - ???

* Breaking changes
    * Removed `AbstractContainer.getParent()`.
    * `Limit.builder()` replaced by `Bucket.builder().addLimit()`.
    * Removed `Container.userDataInToString()`.
* Improvements
    * Performance improvement: Upgraded to Requirements 8.0 to reduce object allocation by `assertThat()`.
    * Added `ConsumptionResult.getTokensLeft()`.

## Version 5.2 - 2022/09/02

* Bug fixes
    * Threads were not sleeping if the duration was less than 1 second.
    * Refill wasn't working properly unless `maxTokens` was a multiple of `refillSize`.
    * `ConfigurationUpdater` wasn't updating all the necessary fields on `close()`.
* Improvements
    * Replaced references to a 3rd-party `@CheckReturnValue` annotation with a local copy.
    * Added ability to remove `userData` from output of `toString()` because it can be very noisy. See
      `Builder.userDataInToString()`.
    * Removed `@CheckReturnValue` from `Container.consume()` and `consume(tokens)` because the number of
      tokens that are consumed is known in advance.
    * Added `toString()` to all `Builder`/`ConfigurationUpdater` classes.
        * Added `ConfigurationUpdater.toString()`.
    * Performance improvement: Group 3+ assertions behind assertionsAreEnabled() to reduce garbage
      creation.

## Version 5.1 - 2022/08/22

* Bug fixes:
    * `ContainerList.ConfigurationUpdater.close()` was not validating the updated children.
* Improvements
    * There is no way to fix a `ConfigurationUpdater` once `try-with-resources` exits, so now the updater is
      closed and write-lock released even if an exception is thrown.

## Version 5.0 - 2022/07/05

* Breaking changes:
    * Renamed `Container.consume(tokens, timeout, unit)` to `Container.tryConsume(tokens, timeout, unit)`.
    * Renamed `Container.consume(minimumTokens, maximumTokens, timeout, unit)` to
      `Container.tryConsume(minimumTokens, maximumTokens, timeout, unit)`.
* Improvements
    * Improved performance of Limit.refill().
    * Container tryConsume() does not wait if `timeout` is zero.

## Version 4.2 - 2022/03/11

* Bugfixes
    * `Bucket.tryConsume()` sometimes returned `longestDelay.getTokensConsumed() > 0` even though
      `tokensConsumed == 0`.

## Version 4.1 - 2022/03/11

* Improvements
    * Added `ConsumptionResult.getConsumedAt()` which denotes the time at which an attempt was made to consume
      tokens. This differs from `getRequestedAt()` in that `getConsumedAt()` is set after acquiring a  
      write-lock.
    * `ConsumptionResult.getAvailableAt()` will be equal to `getConsumedAt()` if tokens were consumed, instead
      of `getRequestedAt()`.
* Bugfixes
    * Documentation typo: `ConsumptionResult.getContainer()` returns "the lowest common ancestor", not
      "the highest common ancestor".
    * Containers that used the same `SelectionPolicy` would corrupt each other's state.
    * `SelectionPolicy` was selecting non-existent children if the number of children in the container was
      reduced.
    * `ContainerList.ConfigurationUpdater.consumeFromOne(), consumeFromAll()` did not update
      `selectionPolicy`.

## Version 4.0 - 2022/02/26

* Breaking changes:
    * Bucket
        * `Builder`/`ConfigurationUpdater.getLimits()` returns a List instead of a Set.
    * ConsumptionResult
        * Renamed `bottleneck` to `bottlenecks`.
    * Renamed `SelectionPolicy.roundRobin()` to `SelectionPolicy.ROUND_ROBIN`.
    * `Limit/Bucket/ContainerList.updateConfiguration()` now holds a lock until the update is applied.
    * Removed `Limit.ConfigurationUpdater.lastRefilledAt/startOfCurrentPeriod/tokensAddedInCurrentPeriod`.
    * Updating a Limit starts a new period.
    * Removed `Bucket.addTokens()` (waiting for someone to bring up a justifying use-case).

* New features
    * Added `ContainerListener` used to listen to Container events.
    * Added `Bucket.getLimitWithLowestRefillRate()`.

* Improvements
    * `ContainerList/Bucket/Limit.toString()` now shows properties on different lines.

* Bugfixes
    * `Limit.refill()` calculation error caused tokensAddedInCurrentPeriod to surpass tokensPerPeriod.
    * `Limit/Bucket/ContainerList.updateConfiguration()` no longer updates the bucket's position in the
      parent's list.

## Version 3.0 - 2022/02/24

* Breaking changes:
    * Limits
        * Renamed `tokensAvailable` to `availableTokens`.
        * Renamed `maxTokens` to `maximumTokens`.
        * Renamed `minimumToRefill` to `minimumRefill`.
        * Renamed `minimumRefill` to `refillSize`.
    * Bucket
        * Retain insertion order of limits.
    * Container
        * Renamed `consumeRange()` to `consume()`.
        * Disallow consumption of zero tokens.
    * ConsumptionResult
        * Renamed `getTokensAvailableAt()` to `getAvailableAt()`.
        * Renamed `getTokensAvailableIn()` to `getAvailableIn()`.
* New features
    * Ability to look up existing values from `Builder`, `ConfigurationUpdater` classes.
    * Ability to navigate from a `Limit` to its `Bucket` and a `Container` to its parent.
    * Added `ConsumptionResult.getBottleneck()` which returns the list of Limits that are preventing
      consumption.
* Improvements:
    * Don't wake up consumers unless the number of tokens is positive.
    * Allow negative `Limit.initialTokens`, `availableTokens`.
* Bugfixes:
    * `ContainerList.updateConfiguration()` was not updating the consumption policy.

## Version 2.0 - 2022/02/21

* Breaking changes:
    * Added `Limit.builder()`, `Bucket.builder()`, `ContainerList.builder()`.
    * `try/consume()` returns `ConsumptionResult` instead of a `boolean`.
* New features:
    * Ability to consume tokens from a container that contains one or more buckets.
    * Ability to consume a variable number of tokens using `try/consumeRange(minimumTokens, maximumTokens)`.
* Bugfix: `maxTokens` may be equal to `tokensPerPeriod` or `initialTokens`.

## Version 1.0 - 2020/02/12

* Initial release