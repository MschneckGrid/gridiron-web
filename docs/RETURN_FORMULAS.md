# Return formulas & performance fee logic

## Modified Dietz method

The dashboard uses Modified Dietz for monthly return calculations.

**Key rule:** The denominator must use weighted average capital, not beginning balance, for months with mid-month contributions. Using beginning balance inflates net returns.

```
Weighted capital = beginning_balance + Σ(cash_flow × (days_remaining / days_in_month))
Net return = (ending_balance - beginning_balance - net_contributions) / weighted_capital
```

Where `net_contributions = contributions - withdrawals` and `days_remaining` = days from the flow date to month end.

## Gross vs net return by fee type

**Internal fee clients:**
- `investment_gain = ending_balance - beginning_balance - contributions + withdrawals`
- `gross_numerator = investment_gain + management_fee` (adds back the fee)
- `gross_return = gross_numerator / weighted_capital`
- `net_return = investment_gain / weighted_capital`

**External fee clients:**
- `gross_return = investment_gain / weighted_capital` (raw Modified Dietz)
- `net_numerator = investment_gain - management_fee` (subtracts fee)
- `net_return = net_numerator / weighted_capital`

## Cash flow direction corrections

When correcting cash flow direction errors (e.g., a contribution was entered as a withdrawal), both the amount field AND the day field for the incorrect direction must be explicitly zeroed out alongside setting the correct direction's fields. Stale day values persist otherwise and cause calculation errors.

## Performance fee / HWM logic

Performance fee clients (e.g., Watson Institute, client_id 27) use a high-water mark (HWM) system:

1. **HWM balance**: The highest ending balance ever achieved
2. **Account gain since HWM**: Current ending balance minus HWM balance
3. **Spread over hurdle**: Account gain minus hurdle amount
4. **Crystallization**: Performance fee is only charged when spread > 0 at quarter end

### Known bug pattern (Watson Q4 2025)

A double-counting bug involving `fee_payment` withdrawal types caused inflated performance fee calculations. The fix required changes in 5 places:
- HWM calculation logic
- Crystallization check passes
- `accountGainSinceHWM` loop
- Invoice display
- Fee payment withdrawal exclusion

The `fee_payment` withdrawal type must be excluded from both the gain calculation and the flow weighting in Modified Dietz.

## Comp Planner formulas

The Mike + Mandy Total row uses pure salary sums — `msPlusMandy` and `dkTotal` must NOT include auto allowance values. A prior bug inflated these totals by double-counting allowances.
