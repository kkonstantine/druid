# OpenTracing Emitter

The OpenTelemetry emitter generates OpenTelemetry Spans for queries.


## Configuration

### Enabling

Load the plugin and enable the emitter in `common.runtime.properties`:

Load the plugin:

```
druid.extensions.loadList=[..., "opentelemetry-emitter"]
```

Enable the emitter:

```
druid.emitter=opentelemetry
```

## Testing

Will be added.
