# Parity: python/LXMF/LXMRouter.py

# Status
- The router now matches the Python 0.9.4 lifecycle: startup config, propagation/peer data persistence (`locally_delivered`, `locally_processed`, `node_stats`), control paths, signal handling, routing jobs, incoming-sync auto-peering only when the remote announce advertises propagation enabled, and message-store cleanup driven only by `message_storage_limit`.
- Delivery-path parity is covered by integration tests for three distinct flows: raw LXMF delivery through `LXMDelivery`, transport callback delivery through `DeliveryPacket`, and direct-link callback wiring after `DeliveryLinkEstablished`, all with end-to-end sender/receiver identity setup and message acceptance on the receiving router.

# TODO
- None.
