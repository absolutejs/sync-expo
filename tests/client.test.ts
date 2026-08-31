import { expect, test } from "bun:test";
import { createMemorySyncLocalStore } from "@absolutejs/sync/client";
import {
  createExpoSyncBridgeLocalStore,
  createExpoSyncBridgeWebSocket,
  type AbsoluteExpoSyncBridgeProvider,
} from "../src/client";
import {
  createExpoSyncBridgeHost,
  createExpoSyncSocketBridgeHost,
} from "../src";

test("WebView proxy preserves native-owned atomicity for one authenticated principal", async () => {
  const host = createExpoSyncBridgeHost({
    namespace: "principal-a",
    store: createMemorySyncLocalStore(),
  });
  const provider: AbsoluteExpoSyncBridgeProvider = {
    on: () => () => undefined,
    request: host.request,
  };
  const store = createExpoSyncBridgeLocalStore({ provider });
  await store.transaction(
    "untrusted-page-namespace",
    "readwrite",
    async (tx) => {
      await tx.setInstallationId("installation-a");
      await tx.putCollection("tasks", {
        cursor: "cursor-1",
        rows: [{ id: 1, title: "offline" }],
        version: 1,
      });
    },
  );
  await expect(
    store.transaction(
      "another-untrusted-namespace",
      "readonly",
      async (tx) => ({
        collection: await tx.getCollection("tasks"),
        installationId: await tx.getInstallationId(),
      }),
    ),
  ).resolves.toEqual({
    collection: {
      cursor: "cursor-1",
      rows: [{ id: 1, title: "offline" }],
      version: 1,
    },
    installationId: "installation-a",
  });
  await expect(
    store.transaction("ignored", "readwrite", async (tx) => {
      await tx.putCollection("rolled-back", { rows: [{ id: 2 }], version: 1 });
      throw new Error("rollback");
    }),
  ).rejects.toThrow("rollback");
  await expect(
    store.transaction("ignored", "readonly", (tx) =>
      tx.getCollection("rolled-back"),
    ),
  ).resolves.toBeUndefined();
  await expect(
    store.transaction("ignored", "readonly", (tx) =>
      tx.putCollection("forbidden", { rows: [], version: 1 }),
    ),
  ).rejects.toThrow();
  await store.deleteNamespace("ignored");
  await expect(
    store.transaction("ignored", "readonly", (tx) => tx.listCollections()),
  ).resolves.toEqual([]);
  await host.close();
});

class FakeNativeWebSocket {
  static readonly OPEN = 1;
  readonly sent: string[] = [];
  readyState = FakeNativeWebSocket.OPEN;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onopen: (() => void) | null = null;

  constructor(readonly url: string) {}
  close(code = 1000, reason = "") {
    this.readyState = 3;
    this.onclose?.({ code, reason });
  }
  send(data: string) {
    this.sent.push(data);
  }
}

test("WebView WebSocket facade exchanges frames while its ticket stays native", async () => {
  const listeners = new Set<(payload: Record<string, unknown>) => void>();
  let nativeSocket: FakeNativeWebSocket | undefined;
  const host = createExpoSyncSocketBridgeHost({
    allowedOrigin: "https://api.example.com",
    emit: (event) => listeners.forEach((listener) => listener(event)),
    socketTicket: async () => "native-secret-ticket",
    webSocketImpl: class extends FakeNativeWebSocket {
      constructor(url: string) {
        super(url);
        nativeSocket = this;
      }
    } as unknown as typeof WebSocket,
  });
  const calls: Array<{ method: string; params: Record<string, unknown> }> = [];
  const provider: AbsoluteExpoSyncBridgeProvider = {
    on: (_event, listener) => {
      listeners.add(listener);

      return () => listeners.delete(listener);
    },
    request: async (method, params) => {
      calls.push({ method, params });

      return host.request(method, params);
    },
  };
  const WebSocketImpl = createExpoSyncBridgeWebSocket(provider);
  const socket = new WebSocketImpl("wss://api.example.com/sync/ws");
  nativeSocket?.onopen?.();
  await Promise.resolve();
  await Promise.resolve();
  expect(nativeSocket?.sent[0]).toContain("native-secret-ticket");
  expect(JSON.stringify(calls)).not.toContain("native-secret-ticket");
  const opened = new Promise<void>((resolve) => {
    if (socket.readyState === WebSocketImpl.OPEN) resolve();
    else socket.onopen = () => resolve();
  });
  await opened;
  socket.send(JSON.stringify({ collection: "tasks", type: "subscribe" }));
  await Promise.resolve();
  await Promise.resolve();
  expect(nativeSocket?.sent.at(-1)).toBe(
    JSON.stringify({ collection: "tasks", type: "subscribe" }),
  );
  const message = new Promise<string>((resolve) => {
    socket.onmessage = (event) => resolve(String(event.data));
  });
  nativeSocket?.onmessage?.({
    data: JSON.stringify({ rows: [{ id: 1 }], type: "snapshot" }),
  });
  await expect(message).resolves.toContain('"snapshot"');
  socket.close();
});
