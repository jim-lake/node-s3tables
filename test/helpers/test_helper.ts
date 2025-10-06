import { test as nodeTest } from 'node:test';
import type { TestContext } from 'node:test';

export interface FailedTestContext extends TestContext {
  failed?: true;
}

export async function test(
  name: string,
  fn: (t: TestContext) => void | Promise<void>
): Promise<void> {
  return _wrapTest(nodeTest, name, fn);
}

async function _wrapTest(
  test_fn: typeof nodeTest,
  name: string,
  fn: (t: TestContext) => void | Promise<void>
): Promise<void> {
  return test_fn(name, async (t: TestContext) => {
    const orig = t.test;
    async function new_func(
      sub_name: string,
      sub_fn: (t: TestContext) => void | Promise<void>
    ): Promise<void> {
      return _wrapTest(orig.bind(t), sub_name, sub_fn);
    }
    // eslint-disable-next-line no-param-reassign
    t.test = Object.assign(new_func, t.test);

    try {
      await fn(t);
    } catch (err) {
      // eslint-disable-next-line no-param-reassign
      (t as FailedTestContext).failed = true;
      throw err;
    }
  });
}
