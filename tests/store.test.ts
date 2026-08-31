import { expect, test } from "bun:test";
import {
  SyncLocalStoreSchemaError,
  type SyncLocalStoreSchemaInput,
} from "@absolutejs/sync/client";
import { assertSyncLocalStoreConformance } from "@absolutejs/sync/testing";
import { createExpoSyncLocalStore, createExpoSyncProtection } from "../src";
import { createFakeExpoSqliteDatabase } from "./support/fakeSqlite";

test("Expo SQLite passes the shared SyncLocalStore contract", async () => {
  const database = createFakeExpoSqliteDatabase();
  await expect(
    assertSyncLocalStoreConformance({
      store: createExpoSyncLocalStore({ database: () => database }),
    }),
  ).resolves.toBeUndefined();
});

test("Expo SQLite uses WAL and one schema statement per native call", async () => {
  const database = createFakeExpoSqliteDatabase();
  const execute = database.execAsync.bind(database);
  const statements: string[] = [];
  database.execAsync = async (statement) => {
    statements.push(statement);
    await execute(statement);
  };
  const store = createExpoSyncLocalStore({ database: () => database });
  await expect(store.getSchemaStatus?.()).resolves.toMatchObject({
    state: "ready",
    storedVersion: 1,
    targetVersion: 1,
  });
  expect(statements[0]).toBe("PRAGMA journal_mode = WAL");
  expect(statements.slice(1)).toHaveLength(6);
  expect(
    statements
      .slice(1)
      .every((statement) => statement.match(/\bCREATE\b/gu)?.length === 1),
  ).toBe(true);
});

test("Expo SQLite encrypts records with a SecureStore-held key", async () => {
  const database = createFakeExpoSqliteDatabase();
  const secureValues = new Map<string, string>();
  const protection = createExpoSyncProtection({
    secureStore: {
      AFTER_FIRST_UNLOCK_THIS_DEVICE_ONLY: 1,
      deleteItemAsync: async (key) => void secureValues.delete(key),
      getItemAsync: async (key) => secureValues.get(key) ?? null,
      isAvailableAsync: async () => true,
      setItemAsync: async (key, value) => void secureValues.set(key, value),
    },
  });
  const storageSchema: SyncLocalStoreSchemaInput = {
    components: [
      {
        id: "@absolutejs/app",
        localData: {
          collections: [{ match: "private", protection: "required" }],
        },
        version: 1,
      },
    ],
  };
  const store = createExpoSyncLocalStore({
    database: () => database,
    protection,
    storageSchema,
  });
  await store.transaction("account-a", "readwrite", (transaction) =>
    transaction.putCollection("private", {
      collection: "private",
      rows: [{ id: 1, secret: "not-on-disk" }],
      version: 1,
    }),
  );
  const raw = await database.getFirstAsync<{ record_json: string }>(
    "SELECT record_json FROM absolute_sync_collections",
  );
  expect(raw?.record_json).toContain("__absoluteSyncProtected");
  expect(raw?.record_json).not.toContain("not-on-disk");
  const reopened = createExpoSyncLocalStore({
    database: () => database,
    protection,
    storageSchema,
  });
  await expect(
    reopened.transaction("account-a", "readonly", (transaction) =>
      transaction.getCollection("private"),
    ),
  ).resolves.toMatchObject({ rows: [{ secret: "not-on-disk" }] });
});

test("Expo SQLite rolls back migration records and version after a crash", async () => {
  const database = createFakeExpoSqliteDatabase();
  const legacy = createExpoSyncLocalStore({ database: () => database });
  await legacy.transaction("account-a", "readwrite", async (transaction) => {
    await transaction.putCollection("first", {
      rows: [{ id: 1 }],
      version: 1,
    });
    await transaction.putCollection("second", {
      rows: [{ id: 2 }],
      version: 1,
    });
  });
  const failed = createExpoSyncLocalStore({
    database: () => database,
    storageSchema: {
      migrations: [
        {
          migrateCollection: (record, context) => {
            if (context.key === "second")
              throw new Error("simulated process death");

            return { ...record, cursor: "partial" };
          },
          toVersion: 2,
        },
      ],
      version: 2,
    },
  });
  await expect(failed.getSchemaStatus?.()).rejects.toThrow(
    "simulated process death",
  );
  const recovered = createExpoSyncLocalStore({
    database: () => database,
    storageSchema: {
      migrations: [{ toVersion: 2 }],
      version: 2,
    },
  });
  await expect(recovered.getSchemaStatus?.()).resolves.toMatchObject({
    storedVersion: 2,
  });
  await expect(
    recovered.transaction("account-a", "readonly", (transaction) =>
      transaction.getCollection("first"),
    ),
  ).resolves.not.toHaveProperty("cursor");
});

test("Expo SQLite rejects an older runtime after a newer schema is committed", async () => {
  const database = createFakeExpoSqliteDatabase();
  await createExpoSyncLocalStore({
    database: () => database,
    storageSchema: {
      migrations: [{ toVersion: 2 }, { toVersion: 3 }],
      version: 3,
    },
  }).getSchemaStatus?.();
  const older = createExpoSyncLocalStore({
    database: () => database,
    storageSchema: { migrations: [{ toVersion: 2 }], version: 2 },
  });
  const error = await older.getSchemaStatus?.().catch((cause) => cause);
  expect(error).toBeInstanceOf(SyncLocalStoreSchemaError);
  expect((error as SyncLocalStoreSchemaError).code).toBe("SCHEMA_TOO_NEW");
});
