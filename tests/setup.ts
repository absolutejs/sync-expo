import { mock } from "bun:test";

mock.module("react-native", () => ({
  AppState: {
    addEventListener: () => ({ remove: () => undefined }),
    currentState: "active",
  },
}));
mock.module("expo-network", () => ({
  addNetworkStateListener: () => ({ remove: () => undefined }),
}));
mock.module("expo-secure-store", () => ({
  AFTER_FIRST_UNLOCK_THIS_DEVICE_ONLY: 1,
  deleteItemAsync: async () => undefined,
  getItemAsync: async () => null,
  isAvailableAsync: async () => true,
  setItemAsync: async () => undefined,
}));
mock.module("expo-sqlite", () => ({
  openDatabaseAsync: async () => {
    throw new Error("Tests must inject an Expo SQLite database.");
  },
}));
mock.module("expo-background-task", () => ({
  BackgroundTaskResult: { Failed: 2, Success: 1 },
  getStatusAsync: async () => 1,
  registerTaskAsync: async () => undefined,
  unregisterTaskAsync: async () => undefined,
}));
mock.module("expo-task-manager", () => ({
  defineTask: () => undefined,
  isAvailableAsync: async () => true,
  isTaskDefined: () => false,
  isTaskRegisteredAsync: async () => false,
}));
