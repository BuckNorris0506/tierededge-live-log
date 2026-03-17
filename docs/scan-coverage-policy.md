# TieredEdge Scan Coverage Policy

TieredEdge uses The Odds API as the primary feed and treats request spend as an operator resource, not a vanity metric.

## Core rules

- Scan Tier A sports first.
- Only expand to Tier B when the core board is sparse.
- Only expand to Tier C with clear request headroom.
- Keep props and alt lines off by default.
- Reuse completed score responses aggressively during grading.

## Tier A

- `basketball_nba`
- `basketball_ncaab`
- `icehockey_nhl`

Default markets:
- `h2h`
- `spreads`
- `totals`

Default books:
- `draftkings`
- `fanduel`
- `betmgm`
- `caesars`

## Tier B

- `baseball_mlb`
- `mma_mixed_martial_arts`
- `soccer_epl`
- `americanfootball_nfl`
- `americanfootball_ncaaf`

## Tier C

Lower-priority sports and leagues only run when budget surplus exists and the core board is too thin to justify stopping.

## Cache windows

- Tier A odds: `10` minutes
- Tier B odds: `20` minutes
- Tier C odds: `30` minutes
- Scores for grading: `360` minutes

## Practical intent

This policy is designed to improve operator value per request, not to maximize raw scan count. It should produce:

- a denser board on the sports that matter most
- fewer wasted requests on low-value leagues
- lower rebuild-time request churn from scores lookups
- clearer explanations for when and why scan expansion happened
