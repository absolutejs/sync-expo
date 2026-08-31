import { expect, test } from "bun:test";
import { installExpoSyncLifecycle } from "../src";

test("flushes on resume and reachable connectivity, then removes listeners", async () => {
  let appListener: ((state: string) => void) | undefined;
  let networkListener:
    | ((state: {
        isConnected?: boolean;
        isInternetReachable?: boolean;
      }) => void)
    | undefined;
  let reconnects = 0;
  let removed = 0;
  const budgets: number[] = [];
  const dispose = installExpoSyncLifecycle({
    client: {
      flush: async ({ timeoutMs } = {}) => {
        budgets.push(timeoutMs ?? -1);

        return { deadLetters: 0, pending: 0, timedOut: false };
      },
      reconnect: () => {
        reconnects += 1;
      },
    },
    dependencies: {
      appState: {
        addEventListener: (_type, listener) => {
          appListener = listener;

          return { remove: () => (removed += 1) };
        },
        currentState: "background",
      },
      network: {
        addNetworkStateListener: (listener) => {
          networkListener = listener;

          return { remove: () => (removed += 1) };
        },
      },
    },
  });
  appListener?.("active");
  networkListener?.({ isConnected: true, isInternetReachable: false });
  networkListener?.({ isConnected: true, isInternetReachable: true });
  await Promise.resolve();
  expect(reconnects).toBe(2);
  expect(budgets).toEqual([10_000, 10_000]);
  dispose();
  dispose();
  expect(removed).toBe(2);
});
