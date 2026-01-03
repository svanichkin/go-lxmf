# go-lxmf

A Go implementation of the LXMF protocol that aims to stay as close as possible (feature and behavior parity) to the original Python library:
`https://github.com/markqvist/LXMF`.

The focus is on reproducible behavior (packing/stamping, routing, propagation, message formats, control requests) so Go and Python nodes interoperate without surprises.

## What's inside

- `lxmf/` — library.
- `cmd/` — `lxmd` daemon (LXM Daemon) for receive/send and (optionally) running as an LXMF Propagation Node.

## Quick start

Build `lxmd`:

```bash
go build -o lxmd ./cmd
```

The first run creates the default config (if missing) and required directories.

Print an example configuration and exit:

```bash
./lxmd -exampleconfig
```

## CLI flags (`lxmd`)

Flags are split into two modes:

1) **Normal daemon mode** — without `-status/-peers/-sync/-break`.
2) **Remote commands to a propagation node** — if any of `-status`, `-peers`, `-sync`, `-break` is set.

### Common flags

- `-config <dir>`: path to the `lxmd` config directory (must contain the `config` file).
- `-rnsconfig <dir>`: path to the Reticulum config directory (passed to `go-reticulum`).
- `-v`, `-verbose`: increase verbosity (can be specified multiple times; accumulates).
- `-q`, `-quiet`: increase quietness (can be specified multiple times; accumulates).
- `-version`: print version (`lxmf.Version`) and exit.
- `-exampleconfig`: print a verbose configuration example and exit.

### Daemon mode flags

- `-propagation-node`: run as an LXMF Propagation Node (even if `propagation.enable_node = no` in the config).
- `-on-inbound <cmd>`: command to run when a message is received. The command gets 1 argument — the full path to the saved message file.
- `-service`: treat `lxmd` as running as a service and log to file (instead of stdout).

### Remote command flags

These commands apply when one or more of the following flags are set:

- `-status`: show node status.
- `-peers`: show peered nodes.
- `-sync <peer_dest_hash>`: request a sync with the specified peer (hex truncated destination hash).
- `-break <peer_dest_hash>`: break peering with the specified peer (hex truncated destination hash).
- `-timeout <seconds>`: timeout for query operations (default: `5`).
- `-remote <dest_hash>`: destination hash of the remote propagation node to send the control request to (hex truncated hash). Required for remote commands.
- `-identity <path>`: path to the identity used for remote requests. Defaults to `identity` next to the config.

Example:

```bash
./lxmd -config ~/.config/lxmd -remote <PN_DEST_HASH_HEX> -status
```

## Configuration keys (`config`)

The file format is INI-like (via `configobj`), matching the original `lxmd`.

### `[lxmf]`

- `display_name` (string, default: `Anonymous Peer`): display name for the delivery destination.
- `announce_at_start` (bool, default: `no`): announce the delivery destination on startup.
- `announce_interval` (int minutes, default: `0`): periodic announce for the delivery destination (0 disables).
- `delivery_transfer_max_accepted_size` (int KB, default: `1000`): maximum accepted unpacked size for messages received directly from peers.
- `on_inbound` (string, default: empty): external command to run on inbound messages (same as `-on-inbound`, but from config).

### `[propagation]`

- `enable_node` (bool, default: `no`): enable propagation node.
- `node_name` (string, default: empty): node name (included in announces).
- `auth_required` (bool, default: `no`): enable authentication; allowed identity hashes are read from the `allowed` file.
- `announce_at_start` (bool, default: `no`): announce the propagation node on startup.
- `announce_interval` (int minutes, default: `0`): periodic announce for the propagation node (0 disables).
- `autopeer` (bool, default: `yes`): automatically peer with other propagation nodes.
- `autopeer_maxdepth` (int, default: `4`): maximum peering depth (in hops) for autopeering.
- `message_storage_limit` (int MB, default: `500`): propagation node message store size limit.
- `propagation_transfer_max_accepted_size` (int KB, default: `256`): max accepted transfer size per incoming propagation transfer (per transfer).
- `propagation_message_max_accepted_size` (int KB, default: `256`): max accepted size for a single incoming propagation message (per message).
- `propagation_sync_max_accepted_size` (int KB, default: `10240`): max accepted transfer size per incoming propagation sync (defaults to `256*40`).
- `propagation_stamp_cost_target` (int, default: `16`): target stamp cost required to deliver messages via this node.
- `propagation_stamp_cost_flexibility` (int, default: `3`): allows accepting lower stamp costs from other propagation nodes (within this range).
- `peering_cost` (int, default: `18`): peering cost required for a remote node to peer with this node.
- `remote_peering_cost_max` (int, default: `26`): maximum peering cost this node will peer with.
- `prioritise_destinations` (comma-separated hex hashes, default: empty): destinations to prioritize in storage.
- `control_allowed` (comma-separated hex identity hashes, default: empty): identity hashes allowed to perform control requests.
- `static_peers` (comma-separated hex destination hashes, default: empty): static peers to always keep peered with.
- `from_static_only` (bool, default: `no`): only accept incoming propagation messages from `static_peers`.
- `max_peers` (int, default: `20`): maximum number of peers to keep.

### `[logging]`

- `loglevel` (int, default: `4`): levels 0..7 (as described in the example config comments).

## Files in the config directory

- `config`: main config file.
- `identity`: daemon identity (auto-created on first run).
- `allowed`: allowed identity hashes (one hex hash per line), used when `propagation.auth_required = yes`.
- `ignored`: ignored LXMF destinations (one hex hash per line).
- `storage/messages`: directory where inbound messages are saved (and router/PN storage).
