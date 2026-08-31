import { expect, test } from "bun:test";
import {
  defineExpoSyncBackgroundTask,
  registerExpoSyncBackgroundTask,
  unregisterExpoSyncBackgroundTask,
  type ExpoSyncBackgroundDependencies,
} from "../src";

const dependencies = () => {
  const definitions = new Map<string, () => Promise<number>>();
  const registered = new Set<string>();
  const value: ExpoSyncBackgroundDependencies = {
    backgroundTask: {
      Failed: 2,
      Success: 1,
      getStatusAsync: async () => 2,
      registerTaskAsync: async (name) => void registered.add(name),
      unregisterTaskAsync: async (name) => void registered.delete(name),
    },
    taskManager: {
      defineTask: (name, run) => void definitions.set(name, run),
      isAvailableAsync: async () => true,
      isTaskDefined: (name) => definitions.has(name),
      isTaskRegisteredAsync: async (name) => registered.has(name),
    },
  };

  return { definitions, registered, value };
};

test("defines, registers, runs, and unregisters one bounded Expo task", async () => {
  const state = dependencies();
  let runs = 0;
  defineExpoSyncBackgroundTask(
    "absolute.sync",
    async () => void (runs += 1),
    state.value,
  );
  await expect(
    registerExpoSyncBackgroundTask(
      "absolute.sync",
      { minimumInterval: 15 },
      state.value,
    ),
  ).resolves.toEqual({ available: true, registered: true, status: 2 });
  expect(await state.definitions.get("absolute.sync")?.()).toBe(1);
  expect(runs).toBe(1);
  await unregisterExpoSyncBackgroundTask("absolute.sync", state.value);
  expect(state.registered.size).toBe(0);
});

test("returns failed without leaking task errors", async () => {
  const state = dependencies();
  defineExpoSyncBackgroundTask(
    "absolute.sync.failed",
    async () => {
      throw new Error("secret backend detail");
    },
    state.value,
  );
  expect(await state.definitions.get("absolute.sync.failed")?.()).toBe(2);
});

test("rejects unsupported sub-fifteen-minute scheduling", async () => {
  const state = dependencies();
  await expect(
    registerExpoSyncBackgroundTask(
      "absolute.sync",
      { minimumInterval: 14 },
      state.value,
    ),
  ).rejects.toThrow("at least 15 minutes");
});
