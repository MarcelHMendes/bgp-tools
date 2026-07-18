# Copilot Instructions for AI-Assisted Development

## Core Principles

### Be Concise
- Provide straightforward, minimal solutions by default
- Only propose complex implementations when absolutely necessary
- Avoid over-engineering; prefer simple, readable code
- If multiple approaches exist, suggest the simplest one first
- Focus on efficiency and token economy in responses

### Clarify When Uncertain
- **Always ask clarifying questions** when commands are ambiguous
- Do not assume intent; request additional context when needed
- Confirm requirements before implementing major changes
- If a request seems incomplete or contradictory, pause and ask

### Clean Code Practices
- Write **self-documenting code** with meaningful variable/function names
- Keep functions **small and focused** (single responsibility)
- **DRY** (Don't Repeat Yourself): extract reusable logic
- Use **consistent formatting** and follow language-specific conventions
- Prefer **composition over inheritance** where appropriate
- Write **tests** for critical functionality when applicable and asked
- Avoid **magic numbers/strings**; use named constants
- Handle **edge cases and errors** gracefully

## Interaction Guidelines

### Response Format
1. **Brief summary** of what you understand
2. **Implementation** (if clear) or **clarifying questions** (if not)
3. **Explanation** of choices (keep it short)

### When to Ask Questions
- If requirements are vague or open to interpretation
- If performance tradeoffs need discussion
- If multiple valid approaches exist
- If the request might break existing functionality

### When to Suggest Complex Solutions
- Only when the problem genuinely requires it
- Explain *why* simplicity won't work
- Provide alternatives if possible
