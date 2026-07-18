"""CryptoStream ingestion service.

Front door of the streaming lane: connects to FreeCryptoAPI over WebSocket,
normalizes each message to the canonical Module 0 §5 contract, publishes
valid ticks to Kafka's `prices` topic, and routes anything malformed to
`prices.dlq`.
"""