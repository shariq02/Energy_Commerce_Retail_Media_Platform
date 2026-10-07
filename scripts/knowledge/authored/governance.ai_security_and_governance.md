# Governance: AI security and AI governance

Security-critical AI behaviour is enforced in code and is never left to a prompt.
- Prompt injection: retrieved and tool content is always tagged as data, never as instructions. Tool authorization is enforced independently of the model.
- Corpus poisoning: the production corpus is curated and platform-owned. An integrity check compares it with the manifest and the last-known-good corpus.
- Malformed tool arguments: the MCP server validates the schema before it runs a tool.
- Unauthorized access: each agent has its own credentials, checked on the server.

- Tool-result poisoning: every answer that cites a metric gets an automatic data-quality check.
- Cross-agent contamination: evidence passed between agents is summarized and cited, never raw.
- Exfiltration and cost: tools return aggregates only, calls are rate limited, every loop has a retry cap, and a cost budget stops a runaway request.

Governance has two layers. For GenAI-assisted engineering, AI-generated output is not authoritative until a human reviews it. The normal tests and static checks apply with no exemption, and architecture decisions are never changed silently. For the AI product, each agent has a versioned prompt lineage. A change touching `ai/` must pass the evaluation matrix. The hosted-model evaluation run is the only authoritative gate before a production deploy. A request that cannot be grounded gets a structured "could not produce a grounded answer" response.
