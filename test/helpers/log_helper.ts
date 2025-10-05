import { test } from 'node:test';
import { basename } from 'node:path';

const g_logs: unknown[][] = [];

test.beforeEach((t) => {
  g_logs.splice(0);
});
test.afterEach((t) => {
  if (t.failed) {
    console.log('FAILED:', `[${basename(t.filePath)}]`, t.fullName);
    g_logs.forEach((args) => console.log(...args));
  }
  g_logs.splice(0);
});
export function log(...args: unknown[]) {
  g_logs.push(args);
}
