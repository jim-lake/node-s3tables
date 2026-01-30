export async function eachLimit<T>(
  items: T[],
  limit: number,
  func: (item: T) => Promise<unknown>
): Promise<void> {
  const executing = new Set<Promise<unknown>>();
  for (const item of items) {
    const p = func(item);
    executing.add(p);

    function clean() {
      executing.delete(p);
    }
    p.then(clean, clean);

    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  await Promise.all(executing);
}
