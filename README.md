[![Master](https://travis-ci.org/jeffgrunewald/stargate.svg?branch=master)](https://travis-ci.org/jeffgrunewald/stargate)
![](https://github.com/jeffgrunewald/stargate/workflows/CI.badge.svg)

# Stargate

An Apache Pulsar client written in Elixir using the Pulsar websocket API.

### NOTE: This package is still in the pre-release phase and is under active development. Stay tuned!

## Installation

Until [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `stargate` to your list of dependencies in `mix.exs` via the Github strategy:

```elixir
def deps do
  [
    {:stargate, github: "jeffgrunewald/stargate"}
  ]
end
```

Once published, the docs can be found at [https://hexdocs.pm/stargate](https://hexdocs.pm/stargate).
