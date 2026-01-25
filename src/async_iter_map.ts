export async function* asyncIterMap<T, R>(
  items: readonly T[],
  limit: number,
  func: (item: T) => Promise<R>
): AsyncIterable<R> {
  interface Result {
    promise: Promise<Result> | undefined;
    value: R | undefined;
  }
  const pending = new Set<Promise<Result>>();

  let index = 0;
  function enqueue() {
    const item = items[index++];
    if (item !== undefined) {
      const result: Result = { promise: undefined, value: undefined };
      const promise = func(item).then((value) => {
        result.value = value;
        return result;
      });
      result.promise = promise;
      pending.add(promise);
    }
  }

  for (let i = 0; i < limit && i < items.length; i++) {
    enqueue();
  }

  while (pending.size) {
    const { promise, value } = await Promise.race(pending);
    if (promise) {
      pending.delete(promise);
    }
    if (value !== undefined) {
      yield value;
    }
    enqueue();
  }
}
