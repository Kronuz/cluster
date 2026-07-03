# Architecture

How the cluster substrate is put together, and the path a datagram takes from the wire to a
protocol handler. Read `README.md` first for the shape and the layering; this is the map.

## The layering

```
      app        its node type + state hooks + apply(command) + its own message types
     ---------------------------- the injected seams ----------------------------------
  cluster   Bus         versioned, token-scoped, typed multicast messaging   [done]
            Raft        term/log/role/votes + election + append + commit     [next]
            gossip      HELLO/WAVE/SNEER/ENTER/BYE naming + the node table    [next]
            length      the varint length + length-prefixed string codec (wire-compat)
     ---------------------------------------------------------------------------------
  reactor   UdpServer / PeriodicTimer / Signal   -- the Asio UDP transport + loop
```

Raft and the membership gossip are two protocols **multiplexed on one multicast socket** (the
`Bus`): every node hears every datagram, and the message *type* selects the handler. They also
share the node table and the cluster state, so they live in one lib (Raft is a separable module,
not a separate repo). The app is injected — it provides the node type, the apply callback, and
the cluster-lifecycle hooks; the substrate never learns the app's domain.

## The wire frame

Every message on the bus is one UDP datagram with a fixed prefix, byte-compatible with Xapiand's
classic discovery transport (so a migrated node interoperates with an un-migrated one):

```
  +--------+--------+--------+------------------------+-----------------+
  | major  | minor  |  type  | serialise_string(token)|     content     |
  |  1 B   |  1 B   |  1 B   |  varint len + token    |   type-specific |
  +--------+--------+--------+------------------------+-----------------+
```

- **major/minor** — the protocol version. A datagram whose version is *newer* than ours is
  dropped (an old node never misparses a new node's frame).
- **type** — the message type (0..max_type). One type space shared by Raft, gossip, and the app.
- **token** — the cluster name/id; a datagram whose token != ours is dropped, so overlapping
  clusters on the same multicast group ignore each other.
- **content** — the rest, parsed by the type's handler.

The `length.h` codec (`serialise_length` / `serialise_string`) is a varint identical to Xapiand's:
a byte `< 255` is the length verbatim; `0xff` introduces a `(len - 255)` base-128 continuation
with the final byte's high bit set. The unserialise side never throws — it returns `false` on a
truncated/overlong frame, and the bus drops it (gossip is best-effort).

## A datagram's life

```
  a node sends bus.send(type, content)
      -> frame [major][minor][type][token][content]  -> reactor::UdpServer multicast send
  every node in the group receives it (IP_MULTICAST_LOOP includes the sender on one host)
      -> UdpServer receive loop -> Bus::on_datagram(bytes, from)
      -> validate: size >= 4, version <= ours, type in range, token == ours   (else drop)
      -> on_message(type, content, from)   ON the bus reactor thread
      -> the app routes by type: Raft handler | gossip handler | app handler
```

## One thread, no locks

The bus runs on a single `reactor::Reactor` (one `io_context`, one thread). The receive loop and
every protocol's timers all run on that one loop, so Raft's term/vote state, the gossip node
table, and the app's handlers are mutated by a single thread and need no locking between the
datagram handler and the timers. This is why the protocols schedule their timers/posts onto
`bus.io()` (`reactor::PeriodicTimer` / `reactor::Signal`), and never touch that state from another
thread. Sends are the one exception — UDP send and receive are independent directions, so
`bus.send()` is safe from any thread — but an ordering-sensitive protocol posts its sends too.

## What lives above (the injected seams)

The app provides, and the substrate never bakes in:
- **the node type** — a serializable identity (`serialise`/`unserialise`/an id), plus its
  app-specific fields (Xapiand's node carries http/remote/replication ports; the substrate
  doesn't care).
- **membership** — how many nodes, quorum, the local node, touch/drop a node (the node *table* is
  the gossip module's, but the app defines the node).
- **the cluster-lifecycle hooks** — "may I participate now?", "I became leader, set up", "apply
  this committed command". These are where an app's state machine plugs into consensus.

Consensus and gossip mechanics stay here; domain policy stays in the app.
