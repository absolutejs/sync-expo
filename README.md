# @absolutejs/sync-expo

Expo-native persistence and lifecycle integration for
[`@absolutejs/sync`](https://github.com/absolutejs/sync).

- Expo SQLite durable cache, outbox, receipts, cursors, and dead letters.
- Principal-partitioned storage with the same migrations and policy contract as
  web and Capacitor.
- AES-256-GCM record protection with a device-only key held by Expo SecureStore.
- AppState and Expo Network wake-up handling.
- Bounded Expo Background Task integration.

AbsoluteJS provisions this package automatically for Expo applications that use
`@absolutejs/sync`. Direct package consumers can use the exported adapter
functions without AbsoluteJS.

Background execution is an acceleration only. Foreground startup, resume, and
connectivity recovery remain authoritative because Android and iOS decide when
deferrable work is allowed to run.
