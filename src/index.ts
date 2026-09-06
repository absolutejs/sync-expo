import type { SyncClient } from "@absolutejs/sync/client";
import type {
  SyncLocalProtectionProvider,
  SyncLocalRecordProtector,
} from "@absolutejs/sync/client";
import { gcm } from "@noble/ciphers/aes.js";
import * as BackgroundTask from "expo-background-task";
import * as Network from "expo-network";
import * as SecureStore from "expo-secure-store";
import * as TaskManager from "expo-task-manager";
import { AppState } from "react-native";
import { expoSyncRandomBytes } from "./crypto";

export * from "./store";
export * from "./bridge";
export { expoSyncRandomId } from "./crypto";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const base64 = (value: Uint8Array) => {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);

  return btoa(binary);
};
const unbase64 = (value: string) =>
  Uint8Array.from(atob(value), (character) => character.charCodeAt(0));

export type ExpoSyncSecureStore = {
  AFTER_FIRST_UNLOCK_THIS_DEVICE_ONLY: number;
  deleteItemAsync(
    key: string,
    options?: Record<string, unknown>,
  ): Promise<void>;
  getItemAsync(
    key: string,
    options?: Record<string, unknown>,
  ): Promise<string | null>;
  isAvailableAsync(): Promise<boolean>;
  setItemAsync(
    key: string,
    value: string,
    options?: Record<string, unknown>,
  ): Promise<void>;
};

export type ExpoSyncProtectionOptions = {
  secureStore?: ExpoSyncSecureStore;
  storagePrefix?: string;
};

const normalizeStoragePrefix = (value = "absolutejs.sync") => {
  if (!/^[A-Za-z0-9._-]{1,80}$/u.test(value))
    throw new TypeError(
      "Expo Sync storagePrefix must use 1-80 letters, numbers, dots, underscores, or hyphens.",
    );

  return value;
};

const lockTails = new Map<string, Promise<void>>();
const withProcessLock = async <T>(key: string, run: () => Promise<T>) => {
  const previous = lockTails.get(key) ?? Promise.resolve();
  let release: () => void = () => undefined;
  const current = new Promise<void>((resolve) => {
    release = resolve;
  });
  const tail = previous.then(() => current);
  lockTails.set(key, tail);
  await previous;
  try {
    return await run();
  } finally {
    release();
    if (lockTails.get(key) === tail) lockTails.delete(key);
  }
};

/** AES-256-GCM records whose data key is retained only by Expo SecureStore. */
export const createExpoSyncProtection = (
  options: ExpoSyncProtectionOptions = {},
): SyncLocalProtectionProvider => {
  const storage = options.secureStore ?? SecureStore;
  const prefix = normalizeStoragePrefix(options.storagePrefix);
  const keyName = `${prefix}.data-key.v1`;
  const secureStoreOptions = {
    keychainAccessible: storage.AFTER_FIRST_UNLOCK_THIS_DEVICE_ONLY,
  };

  return {
    prepare: async (): Promise<SyncLocalRecordProtector> => {
      if (!(await storage.isAvailableAsync()))
        throw new Error(
          "Expo Sync data protection requires persistent SecureStore storage.",
        );
      const key = await withProcessLock(keyName, async () => {
        const existing = await storage.getItemAsync(
          keyName,
          secureStoreOptions,
        );
        if (existing) return unbase64(existing);
        const created = expoSyncRandomBytes(32);
        await storage.setItemAsync(
          keyName,
          base64(created),
          secureStoreOptions,
        );
        const persisted = await storage.getItemAsync(
          keyName,
          secureStoreOptions,
        );
        if (!persisted)
          throw new Error("Expo Sync data-protection key was not persisted.");

        return unbase64(persisted);
      });
      if (key.byteLength !== 32)
        throw new Error("Expo Sync data-protection key is invalid.");
      const additionalData = (context: {
        kind: string;
        name: string;
        namespace: string;
      }) =>
        textEncoder.encode(
          `absolute-sync-v1\u0000${context.kind}\u0000${context.namespace}\u0000${context.name}`,
        );

      return {
        id: "aes-256-gcm-v1",
        open: (value, context) => {
          const bytes = unbase64(value);
          if (bytes.byteLength < 13)
            throw new Error("Expo Sync protected record is malformed.");
          const nonce = bytes.slice(0, 12);

          return textDecoder.decode(
            gcm(key, nonce, additionalData(context)).decrypt(bytes.slice(12)),
          );
        },
        seal: (value, context) => {
          const nonce = expoSyncRandomBytes(12);
          const encrypted = gcm(key, nonce, additionalData(context)).encrypt(
            textEncoder.encode(value),
          );
          const output = new Uint8Array(nonce.length + encrypted.length);
          output.set(nonce);
          output.set(encrypted, nonce.length);

          return base64(output);
        },
      };
    },
  };
};

type Subscription = { remove(): void };
export type ExpoSyncLifecycleDependencies = {
  appState: {
    currentState?: string | null;
    addEventListener(
      type: "change",
      listener: (state: string) => void,
    ): Subscription;
  };
  network: {
    addNetworkStateListener(
      listener: (state: {
        isConnected?: boolean;
        isInternetReachable?: boolean;
      }) => void,
    ): Subscription;
  };
};

export type ExpoSyncLifecycleOptions = {
  client: Pick<SyncClient, "reconnect"> & Partial<Pick<SyncClient, "flush">>;
  /** Finite outbox budget after a wake-up. Defaults to 10 seconds. */
  flushTimeoutMs?: number;
  onError?: (error: unknown) => void;
  dependencies?: ExpoSyncLifecycleDependencies;
};

/** Reconnect and perform a bounded flush after foreground or connectivity. */
export const installExpoSyncLifecycle = ({
  client,
  flushTimeoutMs = 10_000,
  onError,
  dependencies = { appState: AppState, network: Network },
}: ExpoSyncLifecycleOptions) => {
  if (!Number.isFinite(flushTimeoutMs) || flushTimeoutMs < 0)
    throw new TypeError(
      "Expo Sync flushTimeoutMs must be a non-negative number.",
    );
  const wake = () => {
    client.reconnect();
    void client
      .flush?.({ timeoutMs: flushTimeoutMs })
      .catch((error) => onError?.(error));
  };
  let previous = dependencies.appState.currentState ?? undefined;
  const appState = dependencies.appState.addEventListener("change", (state) => {
    if (state === "active" && previous !== "active") wake();
    previous = state;
  });
  const network = dependencies.network.addNetworkStateListener((state) => {
    if (state.isConnected && state.isInternetReachable !== false) wake();
  });
  let active = true;

  return () => {
    if (!active) return;
    active = false;
    appState.remove();
    network.remove();
  };
};

export type ExpoSyncBackgroundDependencies = {
  backgroundTask: {
    Failed: number;
    Success: number;
    getStatusAsync(): Promise<number | null>;
    registerTaskAsync(
      taskName: string,
      options?: { minimumInterval?: number },
    ): Promise<void>;
    unregisterTaskAsync(taskName: string): Promise<void>;
  };
  taskManager: {
    defineTask(taskName: string, run: () => Promise<number>): void;
    isAvailableAsync(): Promise<boolean>;
    isTaskDefined(taskName: string): boolean;
    isTaskRegisteredAsync(taskName: string): Promise<boolean>;
  };
};

const backgroundDependencies = (): ExpoSyncBackgroundDependencies => ({
  backgroundTask: {
    Failed: BackgroundTask.BackgroundTaskResult.Failed,
    Success: BackgroundTask.BackgroundTaskResult.Success,
    getStatusAsync: () => BackgroundTask.getStatusAsync(),
    registerTaskAsync: (taskName, options) =>
      BackgroundTask.registerTaskAsync(taskName, options),
    unregisterTaskAsync: (taskName) =>
      BackgroundTask.unregisterTaskAsync(taskName),
  },
  taskManager: TaskManager,
});

const requireTaskName = (taskName: string) => {
  if (!/^[A-Za-z0-9._-]{1,120}$/u.test(taskName))
    throw new TypeError("Expo Sync background task name is invalid.");
};

/** Define the task at module scope before registering it during app startup. */
export const defineExpoSyncBackgroundTask = (
  taskName: string,
  run: () => Promise<unknown>,
  dependencies = backgroundDependencies(),
) => {
  requireTaskName(taskName);
  if (dependencies.taskManager.isTaskDefined(taskName)) return;
  dependencies.taskManager.defineTask(taskName, async () => {
    try {
      await run();

      return dependencies.backgroundTask.Success;
    } catch {
      return dependencies.backgroundTask.Failed;
    }
  });
};

export type ExpoSyncBackgroundRegistration = {
  available: boolean;
  registered: boolean;
  status: number | null;
};

export const registerExpoSyncBackgroundTask = async (
  taskName: string,
  options: { minimumInterval?: number } = {},
  dependencies = backgroundDependencies(),
): Promise<ExpoSyncBackgroundRegistration> => {
  requireTaskName(taskName);
  if (
    options.minimumInterval !== undefined &&
    (!Number.isFinite(options.minimumInterval) || options.minimumInterval < 15)
  )
    throw new TypeError(
      "Expo Sync background minimumInterval must be at least 15 minutes.",
    );
  const available = await dependencies.taskManager.isAvailableAsync();
  const status = await dependencies.backgroundTask.getStatusAsync();
  if (!available)
    return {
      available: false,
      registered: false,
      status,
    };
  if (!dependencies.taskManager.isTaskDefined(taskName))
    throw new Error(
      "Expo Sync background task must be defined at module scope before registration.",
    );
  if (!(await dependencies.taskManager.isTaskRegisteredAsync(taskName)))
    await dependencies.backgroundTask.registerTaskAsync(taskName, options);

  return {
    available: true,
    registered: true,
    status,
  };
};

export const unregisterExpoSyncBackgroundTask = async (
  taskName: string,
  dependencies = backgroundDependencies(),
) => {
  requireTaskName(taskName);
  if (await dependencies.taskManager.isTaskRegisteredAsync(taskName))
    await dependencies.backgroundTask.unregisterTaskAsync(taskName);
};
