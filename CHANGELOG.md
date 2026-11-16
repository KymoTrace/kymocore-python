# Changelog

## 0.2.0 — 2026-11-16

- Add a common cyclic Core for Plot, Parameters, Watch, Memory and Trace,
  compatible with the KymoCore 8.0.0 DATA/control and trace-v2 wire contracts.
- Add parameter discovery, validated writes/readbacks and compact external CAN
  writes; explicit Watch ranges, all nine types and every acknowledgement;
  periodic memory descriptions and measurements.
- Export deterministic, typed ELF/DWARF symbol maps for named Python variables;
  Studio's existing live-variable browser verifies the matching build ID and
  selects the registered getters without a separate Studio change.
- Add bounded trace recording, context managers and sync/async decorators,
  metadata/dictionary fragmentation, checksums, loss reporting and finite close.
- Add duplex UDP/TCP/serial/MQTT operation, bounded client queues, independent
  stream parsers, partial-write/backpressure handling and borrowed-buffer support.
- Preserve the original Sender API. Reject malformed/overflowing DATA inputs.
- Add an all-features example, compatibility guide and tests against Embedded
  golden fixtures and the separately installed KymoStudio receiver/model.

## 0.1.0

- Initial scalar/XY/XYZ sender with timestamps, CRC and four transports.
