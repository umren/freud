export function formatStats(numbers: number[], total: number): string {
  const avg = numbers.length > 0 ? total / numbers.length : 0;
  return `Count: ${numbers.length}, Sum: ${total}, Avg: ${avg}`;
}
