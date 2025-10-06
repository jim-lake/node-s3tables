import { test } from 'node:test';
import { basename } from 'node:path';
import type { FailedTestContext } from './test_helper';

const g_logs: unknown[][] = [];

test.beforeEach(() => {
  g_logs.splice(0);
});
test.afterEach((t) => {
  const testContext = t as FailedTestContext;
  if (testContext.failed) {
    console.log(
      'FAILED:',
      `[${basename(testContext.filePath ?? '')}]`,
      testContext.fullName
    );
    g_logs.forEach((args) => {
      console.log(...args);
    });
  }
  g_logs.splice(0);
});
export function log(...args: unknown[]) {
  g_logs.push(args);
}
