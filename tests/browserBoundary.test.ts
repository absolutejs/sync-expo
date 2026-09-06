import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

test("WebView subpaths do not import Expo or React Native runtimes", async () => {
  for (const file of ["bridge.ts", "client.ts"]) {
    const source = await readFile(
      join(import.meta.dir, "..", "src", file),
      "utf8",
    );

    expect(source).not.toMatch(/from ["']expo(?:-|["'])/u);
    expect(source).not.toMatch(/from ["']react-native["']/u);
  }
});
