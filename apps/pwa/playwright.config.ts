import { defineConfig } from "@playwright/test"

const PWA_PORT = process.env.LAB_PWA_PORT || "3015"

export default defineConfig({
  testDir: "./e2e",
  timeout: 300_000,
  expect: { timeout: 30_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL: `http://127.0.0.1:${PWA_PORT}`,
    trace: "off",
    video: "off",
  },
  // webServer lo arranca el runner 2A.5 (API+PG+Next) para controlar el orden.
})
