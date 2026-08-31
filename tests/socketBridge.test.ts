import { expect, test } from "bun:test";
import { createExpoSyncSocketBridgeHost } from "../src";

class FakeWebSocket {
  static readonly OPEN = 1;
  readonly sent: string[] = [];
  readyState = FakeWebSocket.OPEN;
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

const Impl = FakeWebSocket as unknown as typeof WebSocket;

test("authenticates natively before exposing an open socket event", async () => {
  const events: Array<Record<string, unknown>> = [];
  let socket: FakeWebSocket | undefined;
  const host = createExpoSyncSocketBridgeHost({
    allowedOrigin: "https://api.example.com",
    emit: (event) => events.push(event),
    socketTicket: async (audience) => {
      expect(audience).toBe("https://api.example.com");

      return "single-use-secret";
    },
    webSocketImpl: class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        socket = this;
      }
    } as unknown as typeof WebSocket,
  });
  await host.request("sync.socket.open", {
    socketId: "socket-1",
    url: "wss://api.example.com/sync/ws",
  });
  expect(socket?.url).toBe(
    "wss://api.example.com/sync/ws?__absolute_auth=ticket",
  );
  socket?.onopen?.();
  await Promise.resolve();
  await Promise.resolve();
  expect(socket?.sent).toEqual([
    '{"ticket":"single-use-secret","type":"authenticate"}',
  ]);
  expect(events).toEqual([{ socketId: "socket-1", type: "open" }]);
  expect(JSON.stringify(events)).not.toContain("single-use-secret");
});

test("forwards ordinary frames and never returns the ticket", async () => {
  const events: Array<Record<string, unknown>> = [];
  let socket: FakeWebSocket | undefined;
  const host = createExpoSyncSocketBridgeHost({
    allowedOrigin: "https://api.example.com",
    emit: (event) => events.push(event),
    socketTicket: async () => "ticket",
    webSocketImpl: class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        socket = this;
      }
    } as unknown as typeof WebSocket,
  });
  expect(
    await host.request("sync.socket.open", {
      socketId: "socket-1",
      url: "wss://api.example.com/sync/ws",
    }),
  ).toBeNull();
  socket?.onopen?.();
  await Promise.resolve();
  await Promise.resolve();
  await host.request("sync.socket.sendChunk", {
    data: btoa('{"type":"subscribe"}'),
    index: 0,
    messageId: "web-1",
    socketId: "socket-1",
    total: 1,
  });
  socket?.onmessage?.({ data: '{"type":"snapshot"}' });
  expect(socket?.sent.at(-1)).toBe('{"type":"subscribe"}');
  expect(events.at(-1)).toEqual({
    data: btoa('{"type":"snapshot"}'),
    index: 0,
    messageId: "native_1",
    socketId: "socket-1",
    total: 1,
    type: "message-chunk",
  });
});

test("chunks and reassembles frames larger than one bridge envelope", async () => {
  const events: Array<Record<string, unknown>> = [];
  let socket: FakeWebSocket | undefined;
  const host = createExpoSyncSocketBridgeHost({
    allowedOrigin: "https://api.example.com",
    emit: (event) => events.push(event),
    socketTicket: async () => "ticket",
    webSocketImpl: class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        socket = this;
      }
    } as unknown as typeof WebSocket,
  });
  await host.request("sync.socket.open", {
    socketId: "socket-large",
    url: "wss://api.example.com/sync/ws",
  });
  socket?.onopen?.();
  await Promise.resolve();
  await Promise.resolve();
  const source = JSON.stringify({ rows: ["é".repeat(20_000)] });
  const bytes = new TextEncoder().encode(source);
  const midpoint = 20_000;
  const chunks = [bytes.slice(0, midpoint), bytes.slice(midpoint)];
  for (let index = 0; index < chunks.length; index += 1) {
    let binary = "";
    for (const byte of chunks[index]!) binary += String.fromCharCode(byte);
    await host.request("sync.socket.sendChunk", {
      data: btoa(binary),
      index,
      messageId: "web-large",
      socketId: "socket-large",
      total: chunks.length,
    });
  }
  expect(socket?.sent.at(-1)).toBe(source);
  socket?.onmessage?.({ data: source });
  const emitted = events.filter((event) => event.type === "message-chunk");
  expect(emitted.length).toBeGreaterThan(1);
  const restored = new Uint8Array(
    emitted.flatMap((event) =>
      [...atob(String(event.data))].map((character) => character.charCodeAt(0)),
    ),
  );
  expect(new TextDecoder().decode(restored)).toBe(source);
});

test("rejects cross-origin, insecure, credential-bearing, and reserved URLs", async () => {
  const host = createExpoSyncSocketBridgeHost({
    allowedOrigin: "https://api.example.com",
    emit: () => undefined,
    socketTicket: async () => "ticket",
    webSocketImpl: Impl,
  });
  for (const url of [
    "ws://api.example.com/sync/ws",
    "wss://other.example.com/sync/ws",
    "wss://user:pass@api.example.com/sync/ws",
    "wss://api.example.com/sync/ws?__absolute_auth=stolen",
  ])
    await expect(
      host.request("sync.socket.open", { socketId: crypto.randomUUID(), url }),
    ).rejects.toThrow();
});

test("fails closed when the native ticket request fails", async () => {
  const events: Array<Record<string, unknown>> = [];
  let socket: FakeWebSocket | undefined;
  const host = createExpoSyncSocketBridgeHost({
    allowedOrigin: "https://api.example.com",
    emit: (event) => events.push(event),
    socketTicket: async () => {
      throw new Error("secret issuer failure");
    },
    webSocketImpl: class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        socket = this;
      }
    } as unknown as typeof WebSocket,
  });
  await host.request("sync.socket.open", {
    socketId: "socket-1",
    url: "wss://api.example.com/sync/ws",
  });
  socket?.onopen?.();
  await Promise.resolve();
  await Promise.resolve();
  expect(events).toContainEqual({ socketId: "socket-1", type: "error" });
  expect(JSON.stringify(events)).not.toContain("issuer failure");
});
