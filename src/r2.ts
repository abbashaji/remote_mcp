// r2.ts
//
// DEPRECATED / UNUSED as of the commit that reintroduced this file's
// content. R2 was dropped from this project because Cloudflare gates R2
// bucket creation behind adding a payment method to the account, even
// though usage within the free tier stays at $0 -- that's a hard no for
// this stack's "no credit card" bar. Object storage now runs through
// GitHub Releases (github_release.ts, for versioned/infrequent
// deliverables) and Backblaze B2 (b2.ts, for arbitrary-key/high-churn
// blobs) instead -- see the revised Section 9 cell prompt.
//
// This file is intentionally left as an inert stub rather than deleted,
// because the connector this revert was performed through has no
// delete-file tool available. It is not imported anywhere in index.ts.
// Safe (and preferable) to delete manually via the GitHub UI -- this
// doesn't touch .github/workflows, so no special token scope is needed.
export {};
