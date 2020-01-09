![](https://github.com/jeffgrunewald/stargate/workflows/CI/badge.svg)

# Stargate

An Apache Pulsar client written in Elixir using the Pulsar websocket API.

Stargate allows you to create producer and consumer connections to a Pulsar
cluster and send messages to or receive messages from the cluster. Receivers
of messages from Pulsar can be a Reader connection, receiving messages from a
topic based on an initial message in the log defined by the user, or via a Consumer
connection, receiving all messages on the topic after initial connection.

Per Pulsar's documentation, all messages received from a topic are acknowledged by the websocket
API, but the Reader acknowledgment is solely to signal to the socket the reader is
ready for additional messages, while Consumer acknowledgement will additionally signal
to the socket the consumer is ready for the message to be deleted from its subscription.

Stargate receivers must supply a message handler module to tell Stargate how to handle
messages received from the socket.

Producers must supply connection settings for a persistent connection or may
supply a single URL signifying the desired persistence, tenant, namespace, and topic
to produce to when ad hoc produce is desired.

## Installation

[Available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `stargate` to your list of dependencies in `mix.exs` via the Github strategy:

```elixir
def deps do
  [
    {:stargate, "~> 0.1.0"}
  ]
end
```

The docs can be found at [https://hexdocs.pm/stargate](https://hexdocs.pm/stargate).

## Usage
### Produce
Producing to Pulsar via Stargate is as simple as passing an Erlang term to the produce function. Stargate
takes care of encoding the message payload with the necessary fields and format required by Pulsar with
options to specify certain fields as fits the users' specific needs (see below).

To ad hoc produce to Pulsar via Stargate, call `Stargate.produce(url, [message])`
where `url` is something like ws://cluster-url:8080/ws/v2/producer/:persistence/:tenant/:namespace/:topic"
and `[message]` is a single message or a list of messages which can be one of:
- a string-keyed map containing the "payload" key and optionally the "context" key if a specific
message context is desired (Stargate uses the "context" for tracking receipt of produced messages by the cluster).
- A two-element tuple containing a key as the first element and a payload as the second.
- A message payload only
Stargate will clean up the producer processes once the produce has completed.

For persistent producer connections to the cluster, you may also start a supervised tree of processes
including the socket producer itself and an acknowledger whos job it is to ensure receipt of messages
by the cluster.

To start a supervised producer, call something like the following within your application:
```elixir
opts = [
  name: my_producer,               optional \\ default :default
  host: [example.com: 8080],
  protocol: "ws",                     optional \\ default ws
  producer: [
    persistence: "persistent",        optional \\ default persistent
    tenant: "public",
    namespace: "default",
    topic: "foo",
    query_params: %{                  all query params are optional
      name: "summer",
      send_timeout: 30_000,             \\ default 30_000
      batch_enabled: true,              \\ default false
      batch_max_msg: 1_000,             \\ default 1_000
      max_pending_msg: 1_000,           \\ default 1_000
      batch_max_delay: 25,              \\ default 10
      routing_mode: :round_robin,       \\ (deprecated by Pulsar) options :round_robin | :single
      compression_type: :lz4,           \\ default uncompressed, options :lz4 | :zlib
      producer_name: "myapp-producer",
      initial_seq_id: 100,
      hashing_scheme: :murmur3          \\ options :java_string | :murmur3
    }
  ]
]
Stargate.Supervisor.start_link(opts)
```

By default, the `Stargate.produce/2` will block until it receives
acknowledgement the message has successfully been produced to the cluster. To ackowledge produce
operations in a non-blocking manner, you may also use `Stargate.produce/3` and pass an MFA tuple
as the third argument instructing the acknowledger which module, function, and arguments to execute
upon receipt of acknowledgement from Pulsar that the produce succeeded.

### Receive (Consume/Read)
To consume messages from Pulsar via Stargate, your application needs to create a consumer or reader
process by calling something like the following:
```elixir
opts = [
  name: my_reader,               optional \\ default :default
  host: [example.com: 8080],
  protocol: "ws",                  optional \\ default ws
  reader: [
    persistence: "persistent",     optional \\ default persistent
    tenant: "public",
    namespace: "default",
    topic: "foo",
    processors: 3,                 optional \\ default 1
    handler: MyApp.Reader.Handler,
    handler_init_args: []          optional \\ default []
    query_params: %{               all query params are optional
      queue_size: 1_000,             \\ default 1_000
      name: "morty",
      starting_message: :latest      \\ default :latest, options :latest | :earliest | "some-message-id"
    }
  ]
]

opts = [
  name: my_consumer,                optional \\ default :default
  host: [example.com: 8080],
  protocol: "ws",                   optional \\ default ws
  consumer: [
    persistence: "persistent",      optional \\ default persistent
    tenant: "public",
    namespace: "default",
    topic: "foo",
    subscription: "my-app",
    processors: 3,                  optional \\ default 1
    handler: MyApp.Reader.Handler,
    handler_init_args: []           optional \\ default []
    query_params: %{                all query params are optional
      subscription_type: :shared,     \\ default :exclusive
      ack_timeout: 100,               \\ default 0
      queue_size: 1_000,              \\ default 1_000
      name: "rick",
      priority: 10,
      max_redeliver_count: 10,        \\ default 0
      dead_letter_topic: "ricks-dlq", \\ default "{topic}-{subscription}-DLQ"
      pull_mode: true                 \\ default false
    }
  ]
]

Stargate.Supervisor.start_link(opts)
```

Stargate's receivers are implemented using GenStage to allow for the message processing (the processes that actually
call your handler function on messages received) to be parallelized as well as to backpressure into the cluster
when setting your consumer connection to `pull_mode: true`.

### Producing AND Receiving
Starting producers and consumer can but _need not_ require different supervision trees. If your application is a link
in a chain that will both receive and produce to the same cluster, you can start a single supervisor with keys for
both the receiver and producer configs in the options passed to the `Stargate.Supervisor.start_link/1` function.

As an example, the configuration below would start a Stargate Supervisor that will manage processes that both monitor an
hypothetical company's internal R&D topic publishing application features ready for release and then turn around and
produce messages about those feature messages to the application marketing team's topic for public consumption.

```elixir
options = [
  name: pulsar-app,
  host: [example.com: 8080],
  producer: [
    persistence: "non-persistent",
    tenant: "marketing",
    namespace: "public",
    topic: "new-stuff"
  ]
  consumer: [
    tenant: "internal",
    namespace: "research",
    topic: "ready-to-release",
    handler: Publicizer.MessageHandler
  ]
]

Stargate.Supervisor.start_link(options)
```

### Receiving Messages
The message handler module passed to a receiver is expected to implement the `Stargate.Receiver.MessageHandler`
behaviour which includes an optional `init/1` if your message handler is expected to track state across
messages received from the topic and `handle_message/1`, `handle_message/2` where `handle_message/1` receives
only a message while `handle_message/2` receives a message and the state being tracked across all messages handled.

Including `use Stargate.Receiver.MessageHandler` in your handler module will created default implementations of
these functions that can be overridden as needed. `handle_message/1` is expected to return either `:ack` or `:continue`
while `handle_message/2` is expected to return `{:ack, state}` or `{:continue, state}`

Messages received by Stargate are automatically converted to a `%Stargate.Message{}` struct by the time they reach
the message handler module. This struct contains all the information about where the message came from (topic, namespace,
tenant, and persistence) as well as the message identifier assigned to it by the Pulsar cluster, the key if one was
supplied, any properties (key/value pairs) attached to the message by the producer, the timestamp the message was published
by the cluster as an Elixir DateTime struct and of course the message payload decoded from the Base64 encoding received from
Pulsar.

## Limitations

### Cumulative acknowledgement
At present, Apache Pulsar does _not_ support cumulative acknowledgement of messages via
the websocket API. When this is supported (PR forthcoming with the upstream project), Stargate
will be updated to include this as an optional ack strategy for faster processing of large groups
of messages.

### Custom topic configuration and administrative functionality
At the time of initial release, Stargate only provides functionality for producing to and
receiving (reading or consuming) from Pulsar topics. Pulsar allows for automatically creating
single-partition topics within existing tenants and namespaces.

While Pulsar provides a RESTful management API, Stargate does not currently provide any
functions for interacting with this API. This _is_ a planned expansion of Stargate functionality
with the intent that management of tenants, namespaces, and topics can all be accomplished
natively from Stargate modules.

Stay tuned!

## Additional information
### Testing
Stargate uses the [divo](https://github.com/smartcitiesdata/divo) and the plugin
[divo_pulsar](https://github.com/jeffgrunewald/divo_pulsar) for integration testing,
using Docker to stand up a single-node Pulsar "cluster" in standalone mode for testing.
See the documentation on the divo and divo_pulsar hex packages for further details.

### Pulsar
For additional information on Pulsar, including configuring and running a cluster,
see the project's [official documentation](https://pulsar.apache.org).
