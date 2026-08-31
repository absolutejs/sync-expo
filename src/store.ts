import type {
  LocalCollectionRecord,
  LocalMutationRecord,
  SyncLocalStore,
  SyncLocalStoreMode,
  SyncLocalStoreSchemaInput,
  SyncLocalStoreSchemaStatus,
  SyncLocalProtectionProvider,
  SyncLocalRecordProtector,
  SyncLocalTransaction,
} from "@absolutejs/sync/client";
import {
  createSyncLocalSchemaStatus,
  migrateSyncLocalCollectionRecord,
  migrateSyncLocalMutationRecord,
  resolveSyncLocalDataPolicy,
  resolveSyncLocalSchemaComponents,
  runSyncLocalPolicyTransaction,
} from "@absolutejs/sync/client";
import * as SQLite from "expo-sqlite";

export type ExpoSyncSqliteExecutor = {
  execAsync(source: string): Promise<void>;
  getAllAsync<T>(
    source: string,
    params?: readonly (boolean | number | null | string | Uint8Array)[],
  ): Promise<T[]>;
  getFirstAsync<T>(
    source: string,
    params?: readonly (boolean | number | null | string | Uint8Array)[],
  ): Promise<T | null>;
  runAsync(
    source: string,
    params?: readonly (boolean | number | null | string | Uint8Array)[],
  ): Promise<unknown>;
};

export type ExpoSyncSqliteDatabase = ExpoSyncSqliteExecutor & {
  withExclusiveTransactionAsync(
    run: (transaction: ExpoSyncSqliteExecutor) => Promise<void>,
  ): Promise<void>;
};

export type ExpoSyncSqliteFactory = () =>
  | ExpoSyncSqliteDatabase
  | Promise<ExpoSyncSqliteDatabase>;

export type ExpoSyncLocalStoreOptions = {
  /** Defaults to `absolutejs-sync-local-v1.db`. */
  databaseName?: string;
  /** Injection seam for conformance tests and custom database provisioning. */
  database?: ExpoSyncSqliteFactory;
  /** Same generated logical migration plan used by web and Capacitor. */
  storageSchema?: SyncLocalStoreSchemaInput;
  protection?: SyncLocalProtectionProvider;
  now?: () => number;
};

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS absolute_sync_schema (
  singleton_id INTEGER PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
  logical_version INTEGER NOT NULL
)`,
  `CREATE TABLE IF NOT EXISTS absolute_sync_schema_components (
  component_id TEXT PRIMARY KEY NOT NULL,
  logical_version INTEGER NOT NULL
)`,
  `CREATE TABLE IF NOT EXISTS absolute_sync_metadata (
  namespace TEXT PRIMARY KEY NOT NULL,
  installation_id TEXT NOT NULL
)`,
  `CREATE TABLE IF NOT EXISTS absolute_sync_collections (
  namespace TEXT NOT NULL,
  collection_key TEXT NOT NULL,
  record_json TEXT NOT NULL,
  PRIMARY KEY (namespace, collection_key)
)`,
  `CREATE TABLE IF NOT EXISTS absolute_sync_mutations (
  namespace TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  record_json TEXT NOT NULL,
  PRIMARY KEY (namespace, operation_id)
)`,
  `CREATE INDEX IF NOT EXISTS absolute_sync_mutations_order
  ON absolute_sync_mutations (namespace, created_at, operation_id)`,
] as const;

type SqliteValue = boolean | number | null | string | Uint8Array;
type SqliteRow = Record<string, unknown>;

const executor = (
  value: Pick<
    SQLite.SQLiteDatabase,
    "execAsync" | "getAllAsync" | "getFirstAsync" | "runAsync"
  >,
): ExpoSyncSqliteExecutor => ({
  execAsync: (source) => value.execAsync(source),
  getAllAsync: (source, params = []) =>
    value.getAllAsync(source, [...params] as SQLite.SQLiteBindParams),
  getFirstAsync: (source, params = []) =>
    value.getFirstAsync(source, [...params] as SQLite.SQLiteBindParams),
  runAsync: (source, params = []) =>
    value.runAsync(source, [...params] as SQLite.SQLiteBindParams),
});

const defaultDatabase = async (
  databaseName: string,
): Promise<ExpoSyncSqliteDatabase> => {
  const database = await SQLite.openDatabaseAsync(databaseName);
  const direct = executor(database);

  return {
    ...direct,
    withExclusiveTransactionAsync: (run) =>
      database.withExclusiveTransactionAsync((transaction) =>
        run(executor(transaction)),
      ),
  };
};

type ProtectedRecordEnvelope = {
  __absoluteSyncProtected: {
    name: string;
    protector: string;
    value: string;
  };
};

const parseRecord = <T>(
  value: unknown,
  label: string,
  context?: { kind: "collection" | "mutation"; namespace: string },
  protector?: SyncLocalRecordProtector,
): T | undefined => {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string")
    throw new Error(`Expo Sync SQLite returned invalid ${label} JSON.`);
  try {
    const parsed: unknown = JSON.parse(value);
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "__absoluteSyncProtected" in parsed
    ) {
      const envelope = (parsed as ProtectedRecordEnvelope)
        .__absoluteSyncProtected;
      if (!context || !protector || protector.id !== envelope.protector)
        throw new Error(
          `Expo Sync ${label} requires unavailable protection provider "${envelope.protector}".`,
        );
      return JSON.parse(
        protector.open(envelope.value, {
          ...context,
          name: envelope.name,
        }),
      ) as T;
    }

    return parsed as T;
  } catch (cause) {
    throw new Error(`Expo Sync SQLite could not parse ${label} JSON.`, {
      cause,
    });
  }
};

const serializeRecord = (
  value: LocalCollectionRecord | LocalMutationRecord,
  context: {
    kind: "collection" | "mutation";
    name: string;
    namespace: string;
  },
  protector?: SyncLocalRecordProtector,
) =>
  protector
    ? JSON.stringify({
        __absoluteSyncProtected: {
          name: context.name,
          protector: protector.id,
          value: protector.seal(JSON.stringify(value), context),
        },
      } satisfies ProtectedRecordEnvelope)
    : JSON.stringify(value);

const requireNamespace = (namespace: string) => {
  if (namespace.length === 0)
    throw new TypeError("Sync local-store namespace must not be empty.");
};

const rowString = (
  row: SqliteRow | null | undefined,
  field: string,
): string | undefined => {
  const value = row?.[field];

  return typeof value === "string" ? value : undefined;
};

const prepareSchema = async (
  database: ExpoSyncSqliteDatabase,
  storageSchema: SyncLocalStoreSchemaInput,
  protector: SyncLocalRecordProtector | undefined,
): Promise<SyncLocalStoreSchemaStatus> => {
  let status: SyncLocalStoreSchemaStatus | undefined;
  await database.withExclusiveTransactionAsync(async (transaction) => {
    const legacy = await transaction.getFirstAsync<SqliteRow>(
      "SELECT logical_version FROM absolute_sync_schema WHERE singleton_id = 1 LIMIT 1",
    );
    const componentRows = await transaction.getAllAsync<SqliteRow>(
      "SELECT component_id, logical_version FROM absolute_sync_schema_components ORDER BY component_id",
    );
    const storedVersions: Record<string, number> = {};
    for (const row of componentRows) {
      if (
        typeof row.component_id !== "string" ||
        typeof row.logical_version !== "number"
      )
        throw new Error(
          "Expo Sync SQLite returned an invalid schema component ledger.",
        );
      storedVersions[row.component_id] = row.logical_version;
    }
    if (
      storedVersions["@absolutejs/app"] === undefined &&
      typeof legacy?.logical_version === "number"
    )
      storedVersions["@absolutejs/app"] = legacy.logical_version;
    const resolved = resolveSyncLocalSchemaComponents(
      storedVersions,
      storageSchema,
    );
    const steps = resolved.components.flatMap((component) => component.steps);
    if (steps.length > 0) {
      const collections = await transaction.getAllAsync<SqliteRow>(
        "SELECT namespace, collection_key, record_json FROM absolute_sync_collections ORDER BY namespace, collection_key",
      );
      for (const row of collections) {
        const namespace = row.namespace;
        const key = row.collection_key;
        if (typeof namespace !== "string" || typeof key !== "string")
          throw new Error(
            "Expo Sync SQLite returned an invalid collection identity.",
          );
        const record = parseRecord<LocalCollectionRecord>(
          row.record_json,
          "collection",
          { kind: "collection", namespace },
          protector,
        );
        if (!record)
          throw new Error(
            "Expo Sync SQLite returned a missing collection record.",
          );
        const migrated = migrateSyncLocalCollectionRecord(
          record,
          { key, namespace },
          steps,
        );
        if (migrated === null)
          await transaction.runAsync(
            "DELETE FROM absolute_sync_collections WHERE namespace = ? AND collection_key = ?",
            [namespace, key],
          );
        else
          await transaction.runAsync(
            "UPDATE absolute_sync_collections SET record_json = ? WHERE namespace = ? AND collection_key = ?",
            [
              serializeRecord(
                migrated,
                {
                  kind: "collection",
                  name: migrated.collection ?? key,
                  namespace,
                },
                protector,
              ),
              namespace,
              key,
            ],
          );
      }
      const mutations = await transaction.getAllAsync<SqliteRow>(
        "SELECT namespace, operation_id, record_json FROM absolute_sync_mutations ORDER BY namespace, operation_id",
      );
      for (const row of mutations) {
        const namespace = row.namespace;
        const operationId = row.operation_id;
        if (typeof namespace !== "string" || typeof operationId !== "string")
          throw new Error(
            "Expo Sync SQLite returned an invalid mutation identity.",
          );
        const record = parseRecord<LocalMutationRecord>(
          row.record_json,
          "mutation",
          { kind: "mutation", namespace },
          protector,
        );
        if (!record)
          throw new Error(
            "Expo Sync SQLite returned a missing mutation record.",
          );
        const migrated = migrateSyncLocalMutationRecord(
          record,
          { key: operationId, namespace },
          steps,
        );
        if (migrated === null)
          await transaction.runAsync(
            "DELETE FROM absolute_sync_mutations WHERE namespace = ? AND operation_id = ?",
            [namespace, operationId],
          );
        else
          await transaction.runAsync(
            "UPDATE absolute_sync_mutations SET created_at = ?, record_json = ? WHERE namespace = ? AND operation_id = ?",
            [
              migrated.createdAt,
              serializeRecord(
                migrated,
                {
                  kind: "mutation",
                  name: migrated.name,
                  namespace,
                },
                protector,
              ),
              namespace,
              operationId,
            ],
          );
      }
    }
    for (const component of resolved.components)
      await transaction.runAsync(
        "INSERT INTO absolute_sync_schema_components (component_id, logical_version) VALUES (?, ?) ON CONFLICT(component_id) DO UPDATE SET logical_version = excluded.logical_version",
        [component.id, component.targetVersion],
      );
    const app = resolved.components.find(
      (component) => component.id === "@absolutejs/app",
    );
    if (app)
      await transaction.runAsync(
        "INSERT INTO absolute_sync_schema (singleton_id, logical_version) VALUES (1, ?) ON CONFLICT(singleton_id) DO UPDATE SET logical_version = excluded.logical_version",
        [app.targetVersion],
      );
    status = createSyncLocalSchemaStatus(
      resolved.components,
      resolved.orphanedComponents,
      "components" in storageSchema,
    );
  });
  if (!status) throw new Error("Expo Sync schema transaction did not run.");

  return status;
};

/**
 * Expo SQLite implementation of Sync's principal-partitioned atomic cache and
 * mutation outbox. Every operation is serialized around an exclusive native
 * transaction so concurrent native routes, WebViews, and background work
 * cannot observe partial state.
 */
export const createExpoSyncLocalStore = ({
  databaseName = "absolutejs-sync-local-v1.db",
  database: createDatabase = () => defaultDatabase(databaseName),
  storageSchema = { version: 1 },
  protection,
  now = Date.now,
}: ExpoSyncLocalStoreOptions = {}): SyncLocalStore => {
  if (!/^[A-Za-z0-9._-]{1,120}$/u.test(databaseName))
    throw new TypeError("Expo Sync databaseName is invalid.");
  const localData = resolveSyncLocalDataPolicy(storageSchema);
  let protectorPromise: Promise<SyncLocalRecordProtector> | undefined;
  const prepareProtector = () => (protectorPromise ??= protection?.prepare());
  let schemaStatus: SyncLocalStoreSchemaStatus | undefined;
  let databasePromise: Promise<ExpoSyncSqliteDatabase> | undefined;
  const database = () => {
    databasePromise ??= Promise.all([
      Promise.resolve(createDatabase()),
      prepareProtector(),
    ]).then(async ([value, protector]) => {
      await value.execAsync("PRAGMA journal_mode = WAL");
      for (const statement of SCHEMA) await value.execAsync(statement);
      schemaStatus = await prepareSchema(value, storageSchema, protector);

      return value;
    });

    return databasePromise;
  };
  let tail = Promise.resolve();
  const locked = async <T>(run: () => Promise<T>): Promise<T> => {
    let release: () => void = () => undefined;
    const previous = tail;
    tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await run();
    } finally {
      release();
    }
  };

  const transaction = async <T>(
    namespace: string,
    mode: SyncLocalStoreMode,
    run: (transaction: SyncLocalTransaction) => Promise<T>,
  ): Promise<T> => {
    requireNamespace(namespace);

    return locked(async () => {
      const value = await database();
      const protector = await prepareProtector();
      let result: T | undefined;
      let completed = false;
      await value.withExclusiveTransactionAsync(async (sqlite) => {
        const writable = () => {
          if (mode !== "readwrite")
            throw new Error(
              "Cannot write in a readonly Sync local transaction",
            );
        };
        const raw: SyncLocalTransaction = {
          deleteCollection: async (key) => {
            writable();
            await sqlite.runAsync(
              "DELETE FROM absolute_sync_collections WHERE namespace = ? AND collection_key = ?",
              [namespace, key],
            );
          },
          deleteMutation: async (operationId) => {
            writable();
            await sqlite.runAsync(
              "DELETE FROM absolute_sync_mutations WHERE namespace = ? AND operation_id = ?",
              [namespace, operationId],
            );
          },
          getCollection: async <R>(key: string) => {
            const row = await sqlite.getFirstAsync<SqliteRow>(
              "SELECT record_json FROM absolute_sync_collections WHERE namespace = ? AND collection_key = ? LIMIT 1",
              [namespace, key],
            );

            return parseRecord<LocalCollectionRecord<R>>(
              row?.record_json,
              "collection",
              { kind: "collection", namespace },
              protector,
            );
          },
          getInstallationId: async () => {
            const row = await sqlite.getFirstAsync<SqliteRow>(
              "SELECT installation_id FROM absolute_sync_metadata WHERE namespace = ? LIMIT 1",
              [namespace],
            );

            return rowString(row, "installation_id");
          },
          getMutation: async (operationId) => {
            const row = await sqlite.getFirstAsync<SqliteRow>(
              "SELECT record_json FROM absolute_sync_mutations WHERE namespace = ? AND operation_id = ? LIMIT 1",
              [namespace, operationId],
            );

            return parseRecord<LocalMutationRecord>(
              row?.record_json,
              "mutation",
              { kind: "mutation", namespace },
              protector,
            );
          },
          listCollections: async () => {
            const rows = await sqlite.getAllAsync<SqliteRow>(
              "SELECT collection_key, record_json FROM absolute_sync_collections WHERE namespace = ? ORDER BY collection_key ASC",
              [namespace],
            );

            return rows
              .map((row) => {
                const key = row.collection_key;
                const record = parseRecord<LocalCollectionRecord>(
                  row.record_json,
                  "collection",
                  { kind: "collection", namespace },
                  protector,
                );

                return typeof key === "string" && record
                  ? { key, record }
                  : undefined;
              })
              .filter(
                (
                  entry,
                ): entry is {
                  key: string;
                  record: LocalCollectionRecord;
                } => entry !== undefined,
              );
          },
          listMutations: async () => {
            const rows = await sqlite.getAllAsync<SqliteRow>(
              "SELECT record_json FROM absolute_sync_mutations WHERE namespace = ? ORDER BY created_at ASC, operation_id ASC",
              [namespace],
            );

            return rows
              .map((row) =>
                parseRecord<LocalMutationRecord>(
                  row.record_json,
                  "mutation",
                  { kind: "mutation", namespace },
                  protector,
                ),
              )
              .filter(
                (record): record is LocalMutationRecord => record !== undefined,
              );
          },
          putCollection: async (key, record) => {
            writable();
            await sqlite.runAsync(
              "INSERT INTO absolute_sync_collections (namespace, collection_key, record_json) VALUES (?, ?, ?) ON CONFLICT(namespace, collection_key) DO UPDATE SET record_json = excluded.record_json",
              [
                namespace,
                key,
                serializeRecord(
                  record,
                  {
                    kind: "collection",
                    name: record.collection ?? key,
                    namespace,
                  },
                  protector,
                ),
              ],
            );
          },
          putMutation: async (record) => {
            writable();
            await sqlite.runAsync(
              "INSERT INTO absolute_sync_mutations (namespace, operation_id, created_at, record_json) VALUES (?, ?, ?, ?) ON CONFLICT(namespace, operation_id) DO UPDATE SET created_at = excluded.created_at, record_json = excluded.record_json",
              [
                namespace,
                record.operationId,
                record.createdAt,
                serializeRecord(
                  record,
                  {
                    kind: "mutation",
                    name: record.name,
                    namespace,
                  },
                  protector,
                ),
              ],
            );
          },
          setInstallationId: async (installationId) => {
            writable();
            if (installationId.length === 0)
              throw new TypeError("Sync installation id must not be empty.");
            await sqlite.runAsync(
              "INSERT INTO absolute_sync_metadata (namespace, installation_id) VALUES (?, ?) ON CONFLICT(namespace) DO UPDATE SET installation_id = excluded.installation_id",
              [namespace, installationId],
            );
          },
        };
        result = await runSyncLocalPolicyTransaction({
          mode,
          now: now(),
          policy: localData,
          protected: protector !== undefined,
          raw,
          run,
        });
        completed = true;
      });
      if (!completed)
        throw new Error("Expo Sync transaction did not complete.");

      return result as T;
    });
  };

  return {
    deleteNamespace: async (namespace) => {
      requireNamespace(namespace);
      await locked(async () => {
        const value = await database();
        await value.withExclusiveTransactionAsync(async (sqlite) => {
          for (const table of [
            "absolute_sync_metadata",
            "absolute_sync_collections",
            "absolute_sync_mutations",
          ])
            await sqlite.runAsync(`DELETE FROM ${table} WHERE namespace = ?`, [
              namespace,
            ]);
        });
      });
    },
    getSchemaStatus: async () => {
      await database();
      if (!schemaStatus) throw new Error("Expo Sync schema was not prepared.");

      return { ...schemaStatus };
    },
    transaction,
  };
};
