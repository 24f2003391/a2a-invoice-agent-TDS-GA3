# A2A Invoice Agent — GA3

This repository contains a FastAPI implementation for the supplied A2A invoice-agent contract.

## Before deploying

1. Create a Gemini API key in Google AI Studio, or adapt `call_ai_for_batch` to another provider.
2. Set `GEMINI_API_KEY` as a Render environment variable. Never commit the key to GitHub.
3. After Render supplies the public HTTPS service URL, set `BASE_URL` to the exact submitted base URL, including `/a2a/`, for example `https://your-service.onrender.com/a2a/`.
4. Keep exactly one Uvicorn worker: the cancellation/result race uses a process-level lock plus SQLite transactions.
5. Submit the exact same base URL in the grader.

## Deploy on Render

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1`

## Endpoints

- Public discovery: `GET /.well-known/agent-card.json`
- Authenticated A2A: `/a2a/message:send`, `/a2a/tasks`, `/a2a/tasks/{id}`, `/a2a/tasks/{id}:cancel`
- Authenticated requests require `Authorization: Bearer <token>` and `A2A-Version: 1.0`.
- `POST /a2a/message:send` additionally requires `Content-Type: application/a2a+json`.

The agent proposes actions only. It creates a receipt artifact only after a result continuation matches stored proposals and contains grader-supplied receipt nonces. Rejected proposals are retained in history but not executed.

## Automated checks

GitHub Actions compiles the app and runs contract tests for the Agent Card, authentication, proposal/receipt lifecycle, idempotency conflicts, user isolation, and the cancellation-versus-receipt race. These tests use a stubbed decision function, so they do not prove AI decision accuracy against the hidden invoice cases.

## Important limitations

The assignment text provided to this repository did not include the actual invoice package examples or the internal document schema. The implementation passes each package as JSON to the model and validates the typed output, but decision accuracy and evidence references must be checked against the real sample packages before using graded Check/Save attempts.

SQLite retains state across normal requests, but a free Render filesystem may be reset on a restart or redeploy. Use a persistent disk or managed database if persistent storage across restarts is required.
