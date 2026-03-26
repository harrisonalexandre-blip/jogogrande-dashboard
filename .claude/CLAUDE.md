# JOGO GRANDE - Dashboard BI

## CRITICAL: Protect API files
The following files are ESSENTIAL for the authentication system and must NEVER be deleted, overwritten, or excluded from commits/deploys:

- `api/login.js` — Login endpoint (Redis-backed)
- `api/users.js` — User management endpoint (Redis-backed)
- `package.json` — Dependencies (ioredis required)

When committing changes (e.g. data updates, dashboard changes), ALWAYS ensure these files remain in the repo. Use `git add` for specific files instead of replacing the entire directory.

## Architecture
- **Frontend**: `index.html` (single-page dashboard with login overlay)
- **Auth API**: `api/login.js` + `api/users.js` (Vercel Serverless Functions, Node.js)
- **User Storage**: Redis (ioredis) via `REDIS_URL` env var (affbr-kv store)
- **Deploy**: Vercel (`npx vercel --prod --yes`)
- **Data**: Smartico API, Phoenix XLSX, DigiToPay CSV

## Deploy checklist
Before running `vercel --prod`, verify:
1. `api/login.js` exists
2. `api/users.js` exists
3. `package.json` has `ioredis` dependency
