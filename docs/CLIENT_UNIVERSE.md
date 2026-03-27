# Client universe & account structure

## Categories (~66 clients)

| Category | Description | Examples |
|----------|-------------|---------|
| General | Standard advisory accounts | Most individual/org accounts |
| P/W (Pension/Welfare) | Union pension and welfare funds | BAC Pension, BAC Welfare, 449 Pension, 449 Welfare |
| HNW | High net worth individuals | Individual client accounts |
| Public | Public/government funds | City of Pitt, City of Wash, Allegheny County |
| Foundation | Foundation accounts | Various charitable foundations |

## Multi-account client structures

Several clients have multiple accounts that represent a single client relationship:

| Client | Accounts | Notes |
|--------|----------|-------|
| BAC Local 9 | BAC Pension, BAC Welfare, BAC General | 3 accounts = 1 client |
| Steamfitters 449 | 449 Pension, 449 Welfare | 2 accounts = 1 client |
| SMW Local 12 | SMW 12 accounts | Multiple accounts |
| Plumbers Local 27 | Plumbers accounts | Multiple accounts |
| Insulators Local 2 | Insulators accounts | Multiple accounts |

## Overlap vs non-overlap accounts

**Only non-overlap accounts anchor client counts per sub-category.** Every multi-account group has exactly one non-overlap account. Overlap + General accounts are excluded from client counts entirely.

This matters for:
- Firm Data tab client count displays
- Form ADV reporting
- Composite reporting membership counts

## Client name matching

**Client name matching in Supabase is case-sensitive enough to require ILIKE** for discovery. Dashboard search term arrays must match substrings of actual `client_name` values in the `clients` table.

## Fee types

Clients are categorized as Internal fee or External fee:
- **Internal fee clients**: Gross return adds back management fee to investment gain before dividing by Modified Dietz denominator
- **External fee clients**: Gross return is raw Modified Dietz; net subtracts fee from numerator

The fee type is stored in the `clients` table and drives the return calculation logic in the Statement tab.

## Quarterly fee data queries

Filter `monthly_data` by `month_end_date` range with `management_fee > 0`, group by `client_name` via JOIN on `clients.client_id`, include `COUNT(*) AS months_found` to detect partial quarters and drive extrapolation (multiply by 3 / months_found).
