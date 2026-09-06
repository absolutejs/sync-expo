import type {
  LocalCollectionRecord,
  LocalMutationRecord,
  SyncLocalStore,
  SyncLocalStoreMode,
  SyncLocalTransaction,
} from "@absolutejs/sync/client";
export type ExpoSyncBridgeHostOptions = {
  store: SyncLocalStore;
  namespace: string;
  /** Maximum time one WebView may hold an atomic transaction. Defaults to 8s. */
  transactionTimeoutMs?: number;
  /** Native random identifier source, normally `expoSyncRandomId`. */
  createId: () => string;
};

export type ExpoSyncSocketBridgeEvent = {
  socketId: string;
  type: "close" | "error" | "message-chunk" | "open";
  code?: number;
  data?: string;
  index?: number;
  messageId?: string;
  reason?: string;
  total?: number;
};

export type ExpoSyncSocketBridgeHostOptions = {
  allowedOrigin: string;
  socketTicket: (audience?: string) => Promise<string>;
  emit(event: ExpoSyncSocketBridgeEvent): void;
  webSocketImpl?: typeof WebSocket;
  maxSockets?: number;
  /** Maximum encoded Sync frame size. Defaults to 4 MiB. */
  maxFrameBytes?: number;
};

type TransactionSession = {
  complete: Promise<void>;
  finish(commit: boolean): void;
  timer: ReturnType<typeof setTimeout>;
  transaction: SyncLocalTransaction;
};

const requireRecord = (value: unknown, label: string) => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new TypeError(`Expo Sync bridge ${label} is invalid.`);

  return value as Record<string, unknown>;
};

const requireString = (value: unknown, label: string) => {
  if (typeof value !== "string" || value.length === 0 || value.length > 512)
    throw new TypeError(`Expo Sync bridge ${label} is invalid.`);

  return value;
};

const requireCollectionRecord = (value: unknown): LocalCollectionRecord => {
  const record = requireRecord(value, "collection record");
  if (
    !Array.isArray(record.rows) ||
    typeof record.version !== "number" ||
    !Number.isSafeInteger(record.version) ||
    record.version < 0
  )
    throw new TypeError("Expo Sync bridge collection record is invalid.");

  return structuredClone(record) as LocalCollectionRecord;
};

const requireMutationRecord = (value: unknown): LocalMutationRecord => {
  const record = requireRecord(value, "mutation record");
  if (
    typeof record.operationId !== "string" ||
    record.operationId.length === 0 ||
    record.operationId.length > 512 ||
    typeof record.name !== "string" ||
    record.name.length === 0 ||
    record.name.length > 512 ||
    typeof record.createdAt !== "number" ||
    !Number.isFinite(record.createdAt) ||
    typeof record.attempts !== "number" ||
    !Number.isSafeInteger(record.attempts) ||
    record.attempts < 0 ||
    !Array.isArray(record.optimistic) ||
    !Array.isArray(record.inverse)
  )
    throw new TypeError("Expo Sync bridge mutation record is invalid.");

  return structuredClone(record) as LocalMutationRecord;
};

const rollbackMarker = Symbol("expo-sync-bridge-rollback");

/**
 * Native owner for WebView local-store transactions. It exposes only Sync's
 * typed persistence contract and never accepts a namespace from page code.
 */
export const createExpoSyncBridgeHost = ({
  store,
  namespace,
  transactionTimeoutMs = 8_000,
  createId,
}: ExpoSyncBridgeHostOptions) => {
  if (!namespace || namespace.length > 512)
    throw new TypeError("Expo Sync bridge namespace is invalid.");
  if (
    !Number.isSafeInteger(transactionTimeoutMs) ||
    transactionTimeoutMs < 100 ||
    transactionTimeoutMs > 30_000
  )
    throw new TypeError(
      "Expo Sync bridge transactionTimeoutMs must be between 100 and 30000.",
    );
  const sessions = new Map<string, TransactionSession>();

  const begin = async (mode: SyncLocalStoreMode) => {
    if (sessions.size >= 8)
      throw new Error("Expo Sync bridge has too many open transactions.");
    const id = createId();
    if (!/^[A-Za-z0-9._:-]{1,160}$/u.test(id) || sessions.has(id))
      throw new Error("Expo Sync bridge generated an invalid transaction id.");
    let readyResolve: (transaction: SyncLocalTransaction) => void = () =>
      undefined;
    let readyReject: (error: unknown) => void = () => undefined;
    const ready = new Promise<SyncLocalTransaction>((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    let finish: (commit: boolean) => void = () => undefined;
    const decision = new Promise<boolean>((resolve) => {
      finish = resolve;
    });
    const complete = store
      .transaction(namespace, mode, async (transaction) => {
        readyResolve(transaction);
        if (!(await decision)) throw rollbackMarker;
      })
      .catch((error) => {
        readyReject(error);
        if (error !== rollbackMarker) throw error;
      });
    const transaction = await ready;
    const timer = setTimeout(() => {
      sessions.delete(id);
      finish(false);
    }, transactionTimeoutMs);
    sessions.set(id, { complete, finish, timer, transaction });

    return id;
  };

  const session = (params: Record<string, unknown>) => {
    const id = requireString(params.transactionId, "transaction id");
    const value = sessions.get(id);
    if (!value)
      throw new Error("Expo Sync bridge transaction is closed or unknown.");

    return { id, value };
  };

  const end = async (params: Record<string, unknown>) => {
    const { id, value } = session(params);
    if (typeof params.commit !== "boolean")
      throw new TypeError("Expo Sync bridge commit decision is invalid.");
    sessions.delete(id);
    clearTimeout(value.timer);
    value.finish(params.commit);
    await value.complete;

    return null;
  };

  const operation = async (
    method: string,
    params: Record<string, unknown>,
  ): Promise<unknown> => {
    const { value } = session(params);
    const transaction = value.transaction;
    if (method === "sync.tx.getInstallationId")
      return (await transaction.getInstallationId()) ?? null;
    if (method === "sync.tx.setInstallationId") {
      await transaction.setInstallationId(
        requireString(params.installationId, "installation id"),
      );

      return null;
    }
    if (method === "sync.tx.getCollection")
      return (
        (await transaction.getCollection(
          requireString(params.key, "collection key"),
        )) ?? null
      );
    if (method === "sync.tx.listCollections")
      return transaction.listCollections();
    if (method === "sync.tx.putCollection") {
      await transaction.putCollection(
        requireString(params.key, "collection key"),
        requireCollectionRecord(params.record),
      );

      return null;
    }
    if (method === "sync.tx.deleteCollection") {
      await transaction.deleteCollection(
        requireString(params.key, "collection key"),
      );

      return null;
    }
    if (method === "sync.tx.listMutations") return transaction.listMutations();
    if (method === "sync.tx.getMutation")
      return (
        (await transaction.getMutation(
          requireString(params.operationId, "operation id"),
        )) ?? null
      );
    if (method === "sync.tx.putMutation") {
      await transaction.putMutation(requireMutationRecord(params.record));

      return null;
    }
    if (method === "sync.tx.deleteMutation") {
      await transaction.deleteMutation(
        requireString(params.operationId, "operation id"),
      );

      return null;
    }
    if (method === "sync.tx.resolveMutationPolicy")
      return (
        transaction.resolveMutationPolicy?.(
          requireString(params.name, "mutation name"),
        ) ?? null
      );
    throw new Error("Expo Sync bridge transaction method is not allowed.");
  };

  return {
    close: async () => {
      const active = [...sessions.values()];
      sessions.clear();
      for (const value of active) {
        clearTimeout(value.timer);
        value.finish(false);
      }
      await Promise.allSettled(active.map((value) => value.complete));
    },
    request: async (method: string, rawParams: unknown): Promise<unknown> => {
      const params = requireRecord(rawParams, "params");
      if (method === "sync.store.begin") {
        if (params.mode !== "readonly" && params.mode !== "readwrite")
          throw new TypeError("Expo Sync bridge transaction mode is invalid.");

        return { transactionId: await begin(params.mode) };
      }
      if (method === "sync.store.end") return end(params);
      if (method === "sync.store.schema")
        return (await store.getSchemaStatus?.()) ?? null;
      if (method === "sync.store.deleteNamespace") {
        await store.deleteNamespace?.(namespace);

        return null;
      }
      if (method.startsWith("sync.tx.")) return operation(method, params);
      throw new Error("Expo Sync bridge method is not allowed.");
    },
  };
};

const websocketOrigin = (url: URL) => {
  const protocol = url.protocol === "wss:" ? "https:" : "http:";

  return `${protocol}//${url.host}`;
};

const ticketSocketUrl = (url: URL) => {
  if (url.searchParams.has("__absolute_auth"))
    throw new TypeError(
      "Expo Sync socket URL contains reserved authentication.",
    );
  url.searchParams.set("__absolute_auth", "ticket");

  return url.href;
};

const SOCKET_CHUNK_BYTES = 24 * 1024;
const SOCKET_UPLOAD_TIMEOUT_MS = 10_000;
const encodeBase64 = (value: Uint8Array) => {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);

  return btoa(binary);
};
const decodeBase64 = (value: string) => {
  if (
    value.length === 0 ||
    value.length > Math.ceil(SOCKET_CHUNK_BYTES / 3) * 4 + 4 ||
    !/^[A-Za-z0-9+/]+={0,2}$/u.test(value)
  )
    throw new TypeError("Expo Sync socket chunk is invalid.");

  return Uint8Array.from(atob(value), (character) => character.charCodeAt(0));
};

/**
 * Owns authenticated sockets in native JavaScript. Only ordinary string Sync
 * frames cross the WebView bridge; the single-use ticket is consumed here.
 */
export const createExpoSyncSocketBridgeHost = ({
  allowedOrigin,
  socketTicket,
  emit,
  webSocketImpl = globalThis.WebSocket,
  maxSockets = 4,
  maxFrameBytes = 4 * 1024 * 1024,
}: ExpoSyncSocketBridgeHostOptions) => {
  const origin = new URL(allowedOrigin);
  if (
    origin.protocol !== "https:" ||
    origin.username ||
    origin.password ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash
  )
    throw new TypeError(
      "Expo Sync socket allowedOrigin must be an HTTPS origin.",
    );
  if (!webSocketImpl)
    throw new Error("Expo Sync socket bridge requires WebSocket support.");
  if (!Number.isSafeInteger(maxSockets) || maxSockets < 1 || maxSockets > 16)
    throw new TypeError("Expo Sync maxSockets must be between 1 and 16.");
  if (
    !Number.isSafeInteger(maxFrameBytes) ||
    maxFrameBytes < SOCKET_CHUNK_BYTES ||
    maxFrameBytes > 16 * 1024 * 1024
  )
    throw new TypeError(
      "Expo Sync maxFrameBytes must be between 24 KiB and 16 MiB.",
    );
  const sockets = new Map<string, WebSocket>();
  const uploads = new Map<
    string,
    {
      chunks: Array<Uint8Array | undefined>;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  let messageSequence = 0;
  const socketId = (value: unknown) => {
    const id = requireString(value, "socket id");
    if (!/^[A-Za-z0-9._:-]{1,160}$/u.test(id))
      throw new TypeError("Expo Sync bridge socket id is invalid.");

    return id;
  };
  const close = (id: string, code?: number, reason?: string) => {
    const socket = sockets.get(id);
    if (!socket) return;
    sockets.delete(id);
    for (const [key, upload] of uploads)
      if (key.startsWith(`${id}:\u0000`)) {
        clearTimeout(upload.timer);
        uploads.delete(key);
      }
    socket.close(code, reason);
  };
  const emitMessage = (id: string, data: string) => {
    const bytes = new TextEncoder().encode(data);
    if (bytes.byteLength > maxFrameBytes) {
      emit({ socketId: id, type: "error" });
      close(id, 1009, "Sync frame is too large");

      return;
    }
    const total = Math.max(1, Math.ceil(bytes.byteLength / SOCKET_CHUNK_BYTES));
    const messageId = `native_${(messageSequence += 1).toString(36)}`;
    for (let index = 0; index < total; index += 1)
      emit({
        data: encodeBase64(
          bytes.slice(
            index * SOCKET_CHUNK_BYTES,
            Math.min(bytes.byteLength, (index + 1) * SOCKET_CHUNK_BYTES),
          ),
        ),
        index,
        messageId,
        socketId: id,
        total,
        type: "message-chunk",
      });
  };

  return {
    close: () => {
      for (const id of [...sockets.keys()]) close(id, 1000, "Host closed");
    },
    request: async (method: string, rawParams: unknown): Promise<unknown> => {
      const params = requireRecord(rawParams, "socket params");
      const id = socketId(params.socketId);
      if (method === "sync.socket.open") {
        if (sockets.has(id))
          throw new Error("Expo Sync bridge socket id is already open.");
        if (sockets.size >= maxSockets)
          throw new Error("Expo Sync bridge socket limit exceeded.");
        const url = new URL(requireString(params.url, "socket URL"));
        if (
          url.protocol !== "wss:" ||
          url.username ||
          url.password ||
          websocketOrigin(url) !== origin.origin
        )
          throw new Error(
            "Expo Sync socket must use WSS on the configured production origin.",
          );
        const socket = new webSocketImpl(ticketSocketUrl(url));
        sockets.set(id, socket);
        socket.onopen = () => {
          void socketTicket(origin.origin)
            .then((ticket) => {
              if (sockets.get(id) !== socket) return;
              socket.send(JSON.stringify({ ticket, type: "authenticate" }));
              emit({ socketId: id, type: "open" });
            })
            .catch(() => {
              if (sockets.get(id) !== socket) return;
              emit({ socketId: id, type: "error" });
              close(id, 1008, "Authentication failed");
            });
        };
        socket.onmessage = (event) => {
          if (sockets.get(id) !== socket) return;
          if (typeof event.data !== "string") {
            emit({ socketId: id, type: "error" });
            close(id, 1003, "Binary frames are not supported");

            return;
          }
          emitMessage(id, event.data);
        };
        socket.onerror = () => {
          if (sockets.get(id) === socket) emit({ socketId: id, type: "error" });
        };
        socket.onclose = (event) => {
          if (sockets.get(id) === socket) sockets.delete(id);
          emit({
            code: event.code,
            reason: event.reason,
            socketId: id,
            type: "close",
          });
        };

        return null;
      }
      if (method === "sync.socket.sendChunk") {
        const socket = sockets.get(id);
        if (!socket || socket.readyState !== webSocketImpl.OPEN)
          throw new Error("Expo Sync bridge socket is not open.");
        const messageId = requireString(params.messageId, "message id");
        const index = params.index;
        const total = params.total;
        if (
          typeof index !== "number" ||
          !Number.isSafeInteger(index) ||
          typeof total !== "number" ||
          !Number.isSafeInteger(total) ||
          index < 0 ||
          total < 1 ||
          index >= total ||
          total > Math.ceil(maxFrameBytes / SOCKET_CHUNK_BYTES)
        )
          throw new TypeError("Expo Sync socket chunk position is invalid.");
        if (typeof params.data !== "string")
          throw new TypeError("Expo Sync socket chunk data is invalid.");
        const key = `${id}:\u0000${messageId}`;
        let upload = uploads.get(key);
        if (!upload) {
          const timer = setTimeout(
            () => uploads.delete(key),
            SOCKET_UPLOAD_TIMEOUT_MS,
          );
          upload = { chunks: Array.from({ length: total }), timer };
          uploads.set(key, upload);
        }
        if (upload.chunks.length !== total || upload.chunks[index])
          throw new Error("Expo Sync socket chunk sequence is invalid.");
        upload.chunks[index] = decodeBase64(params.data);
        if (upload.chunks.every((chunk) => chunk !== undefined)) {
          clearTimeout(upload.timer);
          uploads.delete(key);
          const size = upload.chunks.reduce(
            (sum, chunk) => sum + (chunk?.byteLength ?? 0),
            0,
          );
          if (size > maxFrameBytes)
            throw new Error("Expo Sync socket frame exceeds its byte limit.");
          const bytes = new Uint8Array(size);
          let offset = 0;
          for (const chunk of upload.chunks) {
            bytes.set(chunk!, offset);
            offset += chunk!.byteLength;
          }
          socket.send(new TextDecoder().decode(bytes));
        }

        return null;
      }
      if (method === "sync.socket.close") {
        const code =
          params.code === undefined
            ? undefined
            : typeof params.code === "number" &&
                Number.isSafeInteger(params.code) &&
                params.code >= 1000 &&
                params.code <= 4999
              ? params.code
              : null;
        if (code === null)
          throw new TypeError("Expo Sync bridge close code is invalid.");
        const reason =
          params.reason === undefined
            ? undefined
            : requireString(params.reason, "close reason");
        close(id, code, reason);

        return null;
      }
      throw new Error("Expo Sync socket bridge method is not allowed.");
    },
  };
};
