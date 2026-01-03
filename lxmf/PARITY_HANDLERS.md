# Parity: python/LXMF/Handlers.py

# Status
- Delivery and propagation announce handling now mirrors the Python logic, including stamp cost extraction and propagation peer management.

# TODO
- Ensure `PropagationAnnounceHandler` short-circuits when the router is not configured as a propagation node, matching the Python guard that avoids unpacking PN announces when `propagation_node` is false.
