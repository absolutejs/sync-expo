import * as Crypto from "expo-crypto";

export const expoSyncRandomBytes = (length: number) =>
  Crypto.getRandomBytes(length);

export const expoSyncRandomId = () => Crypto.randomUUID();
