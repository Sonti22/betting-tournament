import Decimal from 'decimal.js';

// 12 integral + 8 fractional amount digits, 6 + 8 rate digits: no rounding.
const Money = Decimal.clone({ precision: 64 });
export function calculatePoints(winAmount: string, rate: string): string {
  return new Money(winAmount).times(rate).toFixed();
}
