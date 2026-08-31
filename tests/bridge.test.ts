import { expect, test } from "bun:test";
import { createMemorySyncLocalStore } from "@absolutejs/sync/client";
import { createExpoSyncBridgeHost } from "../src";

test("commits a typed cross-engine transaction in the native namespace", async () => {
  const store = createMemorySyncLocalStore();
  const bridge = createExpoSyncBridgeHost({
    createId: () => "transaction-1",
    namespace: "principal-a",
    store,
  });
  const opened = (await bridge.request("sync.store.begin", {
    mode: "readwrite",
  })) as { transactionId: string };
  await bridge.request("sync.tx.putCollection", {
    key: "tasks",
    record: { rows: [{ id: 1 }], version: 1 },
    transactionId: opened.transactionId,
  });
  await bridge.request("sync.store.end", {
    commit: true,
    transactionId: opened.transactionId,
  });
  await expect(
    store.transaction("principal-a", "readonly", (transaction) =>
      transaction.getCollection("tasks"),
    ),
  ).resolves.toMatchObject({ rows: [{ id: 1 }] });
  await expect(
    store.transaction("principal-b", "readonly", (transaction) =>
      transaction.getCollection("tasks"),
    ),
  ).resolves.toBeUndefined();
});

test("rolls back every cross-engine write when the page callback fails", async () => {
  const store = createMemorySyncLocalStore();
  const bridge = createExpoSyncBridgeHost({
    createId: () => "transaction-rollback",
    namespace: "principal-a",
    store,
  });
  const opened = (await bridge.request("sync.store.begin", {
    mode: "readwrite",
  })) as { transactionId: string };
  await bridge.request("sync.tx.putCollection", {
    key: "tasks",
    record: { rows: [{ id: 1 }], version: 1 },
    transactionId: opened.transactionId,
  });
  await bridge.request("sync.store.end", {
    commit: false,
    transactionId: opened.transactionId,
  });
  await expect(
    store.transaction("principal-a", "readonly", (transaction) =>
      transaction.getCollection("tasks"),
    ),
  ).resolves.toBeUndefined();
});

test("automatically rolls back an abandoned WebView transaction", async () => {
  const store = createMemorySyncLocalStore();
  const bridge = createExpoSyncBridgeHost({
    createId: () => "transaction-timeout",
    namespace: "principal-a",
    store,
    transactionTimeoutMs: 100,
  });
  const opened = (await bridge.request("sync.store.begin", {
    mode: "readwrite",
  })) as { transactionId: string };
  await bridge.request("sync.tx.putCollection", {
    key: "tasks",
    record: { rows: [{ id: 1 }], version: 1 },
    transactionId: opened.transactionId,
  });
  await Bun.sleep(120);
  await expect(
    bridge.request("sync.tx.getCollection", {
      key: "tasks",
      transactionId: opened.transactionId,
    }),
  ).rejects.toThrow("closed or unknown");
  await expect(
    store.transaction("principal-a", "readonly", (transaction) =>
      transaction.getCollection("tasks"),
    ),
  ).resolves.toBeUndefined();
});

test("never accepts a namespace from WebView-controlled params", async () => {
  const store = createMemorySyncLocalStore();
  const bridge = createExpoSyncBridgeHost({
    createId: () => "transaction-fixed",
    namespace: "principal-a",
    store,
  });
  const opened = (await bridge.request("sync.store.begin", {
    mode: "readwrite",
    namespace: "principal-b",
  })) as { transactionId: string };
  await bridge.request("sync.tx.putMutation", {
    record: {
      args: {},
      attempts: 0,
      createdAt: 1,
      inverse: [],
      name: "tasks:create",
      operationId: "operation-1",
      optimistic: [],
    },
    transactionId: opened.transactionId,
  });
  await bridge.request("sync.store.end", {
    commit: true,
    transactionId: opened.transactionId,
  });
  await expect(
    store.transaction("principal-b", "readonly", (transaction) =>
      transaction.listMutations(),
    ),
  ).resolves.toEqual([]);
  await expect(
    store.transaction("principal-a", "readonly", (transaction) =>
      transaction.listMutations(),
    ),
  ).resolves.toHaveLength(1);
});
