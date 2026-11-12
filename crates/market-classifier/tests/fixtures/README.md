# Fixtures - real Gamma API payloads (captured 2026-11, read-only pull)

Each file embeds the event context (`event_slug`, `event_title`, `event_tags`)
and the verbatim `market` object from
`GET https://gamma-api.polymarket.com/events?tag_slug=hit-price&closed=true&limit=100&offset=N`.

| File | Market | Purpose |
|---|---|---|
| `btc_dip_90k.json` | Will Bitcoin dip to $90,000 by December 31, 2025? | Canonical verified dip (touch-low, explicit window, resolved YES) |
| `eth_dip_monthly.json` | Will Ethereum dip to $3,000 by December 31? | Verified dip; barrier stated with decimals in description |
| `sol_dip_out_of_scope.json` | Will Solana dip to $140 in May? | Out-of-scope asset rejection |
| `btc_reach_out_of_scope.json` | Will Bitcoin reach $1,000,000 …? | Reach (touch-high) family - out of dip-registry scope |

Do not hand-edit the `market` objects: they are evidence. Test mutations
(strip fields, swap barriers) are done in test code on cloned payloads.
