import type {
  LocalCollectionRecord,
  LocalMutationRecord,
  SyncLocalStore,
  SyncLocalStoreMode,
  SyncLocalStoreSchemaInput,
  SyncLocalStoreSchemaStatus,
  SyncLocalTransaction,
} from "@absolutejs/sync/client";
import {
  resolveSyncLocalDataPolicy,
  resolveSyncLocalMutationPolicy,
} from "@absolutejs/sync/client";

export type AbsoluteExpoSyncBridgeProvider = {
  on(
    event: "sync.socket",
    listener: (payload: Record<string, unknown>) => void,
  ): () => void;
  request(method: string, params: Record<string, unknown>): Promise<unknown>;
};

export type ExpoSyncBridgeLocalStoreOptions = {
  provider: AbsoluteExpoSyncBridgeProvider;
  storageSchema?: SyncLocalStoreSchemaInput;
};

const record = (value: unknown, label: string) => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new TypeError(`Expo Sync bridge returned invalid ${label}.`);

  return value as Record<string, unknown>;
};

const transactionId = (value: unknown) => {
  const id = Reflect.get(record(value, "transaction"), "transactionId");
  if (typeof id !== "string" || id.length === 0)
    throw new TypeError("Expo Sync bridge returned an invalid transaction id.");

  return id;
};

const nullable = <T>(value: unknown): T | undefined =>
  value === null || value === undefined ? undefined : (value as T);

/**
 * SyncLocalStore proxy whose atomic transaction is owned by native SQLite.
 * The namespace argument never crosses the bridge; the native Auth principal
 * selected when the host was created is authoritative.
 */
export const createExpoSyncBridgeLocalStore = ({
  provider,
  storageSchema = { version: 1 },
}: ExpoSyncBridgeLocalStoreOptions): SyncLocalStore => {
  const policy = resolveSyncLocalDataPolicy(storageSchema);
  const transaction = async <T>(
    _namespace: string,
    mode: SyncLocalStoreMode,
    run: (transaction: SyncLocalTransaction) => Promise<T>,
  ) => {
    const id = transactionId(
      await provider.request("sync.store.begin", { mode }),
    );
    const request = (method: string, params: Record<string, unknown> = {}) =>
      provider.request(method, { ...params, transactionId: id });
    const remote: SyncLocalTransaction = {
      deleteCollection: async (key) => {
        await request("sync.tx.deleteCollection", { key });
      },
      deleteMutation: async (operationId) => {
        await request("sync.tx.deleteMutation", { operationId });
      },
      getCollection: async <R>(key: string) =>
        nullable<LocalCollectionRecord<R>>(
          await request("sync.tx.getCollection", { key }),
        ),
      getInstallationId: async () =>
        nullable<string>(await request("sync.tx.getInstallationId")),
      getMutation: async (operationId) =>
        nullable<LocalMutationRecord>(
          await request("sync.tx.getMutation", { operationId }),
        ),
      listCollections: async () =>
        (await request("sync.tx.listCollections")) as Array<{
          key: string;
          record: LocalCollectionRecord;
        }>,
      listMutations: async () =>
        (await request("sync.tx.listMutations")) as LocalMutationRecord[],
      putCollection: async (key, collectionRecord) => {
        await request("sync.tx.putCollection", {
          key,
          record: collectionRecord,
        });
      },
      putMutation: async (mutationRecord) => {
        await request("sync.tx.putMutation", { record: mutationRecord });
      },
      resolveMutationPolicy: (name) =>
        resolveSyncLocalMutationPolicy(policy, name),
      setInstallationId: async (installationId) => {
        await request("sync.tx.setInstallationId", { installationId });
      },
    };
    try {
      const result = await run(remote);
      await provider.request("sync.store.end", {
        commit: true,
        transactionId: id,
      });

      return result;
    } catch (error) {
      await provider
        .request("sync.store.end", {
          commit: false,
          transactionId: id,
        })
        .catch(() => undefined);
      throw error;
    }
  };

  return {
    deleteNamespace: async () => {
      await provider.request("sync.store.deleteNamespace", {});
    },
    getSchemaStatus: async () =>
      nullable<SyncLocalStoreSchemaStatus>(
        await provider.request("sync.store.schema", {}),
      ) ??
      (() => {
        throw new Error("Expo Sync native schema status is unavailable.");
      })(),
    transaction,
  };
};

const SOCKET_CHUNK_BYTES = 24 * 1024;
const MAX_SOCKET_FRAME_BYTES = 4 * 1024 * 1024;
const encodeBase64 = (value: Uint8Array) => {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);

  return btoa(binary);
};
const decodeBase64 = (value: string) =>
  Uint8Array.from(atob(value), (character) => character.charCodeAt(0));

type PendingMessage = {
  chunks: Array<Uint8Array | undefined>;
  timer: ReturnType<typeof setTimeout>;
};

/**
 * Browser-compatible string WebSocket facade backed by the native socket host.
 * Default JSON Sync frames are supported; binary serializers fail explicitly.
 */
export const createExpoSyncBridgeWebSocket = (
  provider: AbsoluteExpoSyncBridgeProvider,
): typeof WebSocket => {
  class ExpoBridgeWebSocket extends EventTarget implements WebSocket {
    static readonly CLOSED = 3;
    static readonly CLOSING = 2;
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    readonly CLOSED = 3;
    readonly CLOSING = 2;
    readonly CONNECTING = 0;
    readonly OPEN = 1;
    binaryType: BinaryType = "blob";
    readonly bufferedAmount = 0;
    readonly extensions = "";
    onclose: ((this: WebSocket, event: CloseEvent) => unknown) | null = null;
    onerror: ((this: WebSocket, event: Event) => unknown) | null = null;
    onmessage:
      | ((this: WebSocket, event: MessageEvent<unknown>) => unknown)
      | null = null;
    onopen: ((this: WebSocket, event: Event) => unknown) | null = null;
    readonly protocol = "";
    readyState = ExpoBridgeWebSocket.CONNECTING;
    readonly url: string;
    readonly #socketId = `websocket_${crypto.randomUUID()}`;
    readonly #messages = new Map<string, PendingMessage>();
    readonly #remove: () => void;
    #sendSequence = 0;
    #sendTail = Promise.resolve();

    constructor(url: string | URL, protocols?: string | string[]) {
      super();
      if (
        protocols !== undefined &&
        (typeof protocols !== "string" || protocols.length > 0) &&
        (!Array.isArray(protocols) || protocols.length > 0)
      )
        throw new TypeError(
          "Expo Sync bridge does not support WebSocket subprotocols.",
        );
      this.url = String(url);
      this.#remove = provider.on("sync.socket", (payload) => {
        if (payload.socketId !== this.#socketId) return;
        this.#receive(payload);
      });
      void provider
        .request("sync.socket.open", {
          socketId: this.#socketId,
          url: this.url,
        })
        .catch(() => this.#fail());
    }

    close(code?: number, reason?: string): void {
      if (
        this.readyState === ExpoBridgeWebSocket.CLOSED ||
        this.readyState === ExpoBridgeWebSocket.CLOSING
      )
        return;
      this.readyState = ExpoBridgeWebSocket.CLOSING;
      void provider
        .request("sync.socket.close", {
          ...(code === undefined ? {} : { code }),
          ...(reason === undefined ? {} : { reason }),
          socketId: this.#socketId,
        })
        .catch(() => this.#closed(code ?? 1006, reason ?? ""));
    }

    ping(): void {
      throw new TypeError("Expo Sync bridge WebSocket ping is not supported.");
    }

    send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
      if (this.readyState !== ExpoBridgeWebSocket.OPEN)
        throw new DOMException("WebSocket is not open.", "InvalidStateError");
      if (typeof data !== "string")
        throw new TypeError(
          "Expo Sync bridge supports the default string serializer only.",
        );
      const bytes = new TextEncoder().encode(data);
      if (bytes.byteLength > MAX_SOCKET_FRAME_BYTES)
        throw new DOMException(
          "Sync frame exceeds 4 MiB.",
          "QuotaExceededError",
        );
      const total = Math.max(
        1,
        Math.ceil(bytes.byteLength / SOCKET_CHUNK_BYTES),
      );
      const messageId = `web_${(this.#sendSequence += 1).toString(36)}`;
      this.#sendTail = this.#sendTail
        .then(async () => {
          for (let index = 0; index < total; index += 1)
            await provider.request("sync.socket.sendChunk", {
              data: encodeBase64(
                bytes.slice(
                  index * SOCKET_CHUNK_BYTES,
                  Math.min(bytes.byteLength, (index + 1) * SOCKET_CHUNK_BYTES),
                ),
              ),
              index,
              messageId,
              socketId: this.#socketId,
              total,
            });
        })
        .catch(() => this.#fail());
    }

    #dispatch<T extends Event>(
      event: T,
      listener: ((this: WebSocket, event: T) => unknown) | null,
    ) {
      this.dispatchEvent(event);
      listener?.call(this, event);
    }

    #closed(code: number, reason: string) {
      if (this.readyState === ExpoBridgeWebSocket.CLOSED) return;
      this.readyState = ExpoBridgeWebSocket.CLOSED;
      this.#remove();
      for (const message of this.#messages.values())
        clearTimeout(message.timer);
      this.#messages.clear();
      this.#dispatch(new CloseEvent("close", { code, reason }), this.onclose);
    }

    #fail() {
      if (this.readyState === ExpoBridgeWebSocket.CLOSED) return;
      this.#dispatch(new Event("error"), this.onerror);
      this.#closed(1006, "");
    }

    #receive(payload: Record<string, unknown>) {
      if (payload.type === "open") {
        if (this.readyState !== ExpoBridgeWebSocket.CONNECTING) return;
        this.readyState = ExpoBridgeWebSocket.OPEN;
        this.#dispatch(new Event("open"), this.onopen);

        return;
      }
      if (payload.type === "error") {
        this.#dispatch(new Event("error"), this.onerror);

        return;
      }
      if (payload.type === "close") {
        this.#closed(
          typeof payload.code === "number" ? payload.code : 1006,
          typeof payload.reason === "string" ? payload.reason : "",
        );

        return;
      }
      if (payload.type !== "message-chunk") return;
      const messageId = payload.messageId;
      const index = payload.index;
      const total = payload.total;
      const data = payload.data;
      if (
        typeof messageId !== "string" ||
        typeof index !== "number" ||
        !Number.isSafeInteger(index) ||
        typeof total !== "number" ||
        !Number.isSafeInteger(total) ||
        typeof data !== "string" ||
        index < 0 ||
        total < 1 ||
        index >= total ||
        total > Math.ceil(MAX_SOCKET_FRAME_BYTES / SOCKET_CHUNK_BYTES)
      ) {
        this.#fail();

        return;
      }
      let message = this.#messages.get(messageId);
      if (!message) {
        message = {
          chunks: Array.from({ length: total }),
          timer: setTimeout(() => this.#fail(), 10_000),
        };
        this.#messages.set(messageId, message);
      }
      if (message.chunks.length !== total || message.chunks[index]) {
        this.#fail();

        return;
      }
      try {
        message.chunks[index] = decodeBase64(data);
      } catch {
        this.#fail();

        return;
      }
      if (!message.chunks.every((chunk) => chunk !== undefined)) return;
      clearTimeout(message.timer);
      this.#messages.delete(messageId);
      const size = message.chunks.reduce(
        (sum, chunk) => sum + (chunk?.byteLength ?? 0),
        0,
      );
      if (size > MAX_SOCKET_FRAME_BYTES) {
        this.#fail();

        return;
      }
      const bytes = new Uint8Array(size);
      let offset = 0;
      for (const chunk of message.chunks) {
        bytes.set(chunk!, offset);
        offset += chunk!.byteLength;
      }
      this.#dispatch(
        new MessageEvent("message", {
          data: new TextDecoder().decode(bytes),
        }),
        this.onmessage,
      );
    }
  }

  return ExpoBridgeWebSocket as unknown as typeof WebSocket;
};
