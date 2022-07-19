# Alpaka Typesense notes

## Retries
Retry strategy can be added to settings like in Google modules
```retry-settings {
max-retries = 6
min-backoff = 1 second
max-backoff = 1 minute
random-factor = 0.2
}
```

Flows and sinks can use settings with retry strategy (can be implemented by RetryFlow.withBackoff).

Should be discriminated if retry has any sense - maybe by response code similar to ToThrowableUnmarshallerRetryHelpers.

**Simple request can use just settings without retries (RequestLevelSettings) - is it a good idea?**

## Error handling
Strategy in settings too? But how to define proper strategy?

The most flexible way from the user point of view is to return Try[T] instead T in flow/sink, but it doesn't implemented this way in others modules.

**Must be investigated**

## Tests
**What should be in test application?**

**Can be also test in project**
