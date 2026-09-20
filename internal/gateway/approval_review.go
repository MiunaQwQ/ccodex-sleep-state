package gateway

// Codex selects this dedicated model for its approval reviewer. It must reach
// the upstream reviewer unchanged: no chat ticket, collection, model rewrite,
// local approval decision, or retry. It is not a selectable ticket model.
const approvalReviewModel = "codex-auto-review"
