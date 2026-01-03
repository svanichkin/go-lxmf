# Parity: python/LXMF/LXMRouter.py

# TODO

- Propagation request handlers and flow: `offer_request`, `message_get_request`, `message_list_response`, `message_get_failed`, `propagation_packet`, `propagation_link_established`, `propagation_transfer_signalling_packet`, `acknowledge_sync_completion`, `request_messages_path_job`, `request_messages_from_propagation_node`, `cancel_propagation_node_requests`.
- Propagation store maintenance: `information_storage_size`, `clean_message_store`, throttled peer cleanup, size/limit enforcement, retain-synced logic.
- Peer sync/rotation/queues: `flush_queues`, `sync_peers`, `rotate_peers`, peer distribution queue mapping, acceptance rate/backoff logic.
- Outbound processing parity: backchannel usage, link establishment retries, pathless attempts, propagation-stamp flow, resource send/receipt handling, failure transitions and delivery attempt limits.
- Deferred stamp generation: actual stamp generation via `LXStamper`, cancellation, throttling, propagation-stamp generation path.
- Inbound delivery parity: full stamp enforcement branches (`_enforce_stamps`, `no_stamp_enforcement`), allow/deny/auth checks, locally processed/delivered caches update + persistence.
- Physical stats parity: reticulum cache lookups for RSSI/SNR/Q when packet fields are nil.
- Persistence + lifecycle: save `local_deliveries`, `locally_processed`, `peers`, `node_stats`; exit handler/signal handling for graceful shutdown.
