export async function* asyncIterMap<T, R>(
  items: readonly T[],
  func: (item: T) => Promise<R>
): AsyncIterable<R> {
  type Wrapper = Promise<{ self: Wrapper | undefined; value: R }>;
  const pending = new Set<Wrapper>();

  for (const item of items) {
    const ref: { current?: Wrapper } = {};
    const wrapper = func(item).then((value) => ({
      self: ref.current,
      value,
    })) as Wrapper;
    ref.current = wrapper;
    pending.add(wrapper);
  }

  while (pending.size) {
    const { self, value } = await Promise.race(pending);
    if (self) {
      pending.delete(self);
    }
    yield value;
  }
}
