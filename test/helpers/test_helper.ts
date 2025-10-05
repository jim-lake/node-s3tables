import { test as nodeTest } from 'node:test';
import type { TestContext } from 'node:test';

export interface FailedTestContext extends TestContext {
  failed?: true;
}
export async function test(
  name: string,
  fn: (t: FailedTestContext) => void | Promise<void>
): void | Promise<void> {
  return _wrapTest(nodeTest, name, fn);
}
async function _wrapTest(
  test_fn: (
    name: string,
    fn: (t: FailedTestContext) => void | Promise<void>
  ) => void | Promise<void>,
  name: string,
  fn: (t: TestContext) => void | Promise<void>
): void | Promise<void> {
  return test_fn(name, async (t: TestContext) => {
    const orig = t.test.bind(t);
    if (orig) {
      const new_func = (
        sub_name: string,
        sub_fn: (t: TestContext) => void | Promise<void>
      ) => {
        return _wrapTest(orig, sub_name, sub_fn);
      };
      t.test = Object.assign(new_func, t.test);
    }

    try {
      return await fn(t);
    } catch (err) {
      (t as FailedTestContext).failed = true;
      throw err;
    }
  });
}
