import { Database } from "bun:sqlite";
import type {
  ExpoSyncSqliteDatabase,
  ExpoSyncSqliteExecutor,
} from "../../src/store";

type SqliteValue = boolean | number | null | string | Uint8Array;
const values = (input: readonly SqliteValue[]) =>
  input.map((value) => (typeof value === "boolean" ? Number(value) : value));

/** Runs the narrow Expo adapter contract against real in-memory SQLite. */
export const createFakeExpoSqliteDatabase = (): ExpoSyncSqliteDatabase => {
  const database = new Database(":memory:", { strict: true });
  const executor = (): ExpoSyncSqliteExecutor => ({
    execAsync: async (source) => {
      database.exec(source);
    },
    getAllAsync: async <T>(source: string, params = []) =>
      database.query(source).all(...values(params)) as T[],
    getFirstAsync: async <T>(source: string, params = []) =>
      (database.query(source).get(...values(params)) as T | null) ?? null,
    runAsync: async (source, params = []) =>
      database.query(source).run(...values(params)),
  });

  return {
    ...executor(),
    withExclusiveTransactionAsync: async (run) => {
      database.exec("BEGIN IMMEDIATE");
      try {
        await run(executor());
        database.exec("COMMIT");
      } catch (error) {
        database.exec("ROLLBACK");
        throw error;
      }
    },
  };
};
