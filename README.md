# rover
Explorer

## Project structure

- `app.py` - runtime entrypoint (creates Flask app)
- `app/`
  - `__init__.py` - app factory + global error handler
  - `core/` - shared infrastructure modules
    - `config.py` - app/session configuration
    - `auth.py` - auth guard, CSRF helpers, session auth utilities
    - `db.py` - Oracle connection + schema validation + SQL explorer validation
  - `projects/` - feature-first project packages
    - `auth/routes.py` - login/logout routes
    - `core/routes.py` - health, home, dashboard, city directory routes
    - `sql/routes.py` - SQL explorer routes
    - `kv/routes.py` + `kv/repository.py` - Key-Value routes/data access
    - `sb/routes.py` + `sb/repository.py` - Secondary Brain routes/data access
- `templates/`
  - `shared/` - shared templates (`base.html`, `error.html`)
  - `auth/` - auth templates
  - `dashboard/` - dashboard templates
  - `sql/` - SQL Explorer templates
  - `kv/` - Key-Value templates
  - `sb/` - Secondary Brain templates
  - `legacy/` - city directory templates

## Authentication

This app uses a **single shared login token** with Flask session authentication.

### Required environment variables

- `APP_LOGIN_TOKEN` - secret token you enter on `/login`
- `APP_SECRET_KEY` - Flask session signing key

### Optional environment variable

- `SESSION_COOKIE_SECURE` - `true|false` (default: `false`; set to `true` behind HTTPS)

### Behavior

- Public route: `GET /health`
- Protected routes: `GET /`, `GET /dashboard`, `GET /cities`
- Auth routes:
  - `GET/POST /login`
  - `POST /logout`

## Dashboard (Protected)

- `GET /dashboard` - shows project cards dynamically from `kv_store`
- `GET /` defaults to dashboard after login

Dashboard data source:
- `kv_store` rows with:
  - `category='dashboard'`
  - `is_active='Y'`
- `item_key` is used as slug/path (`/<item_key>`)
- `item_value` is used as display title

Example dashboard records:
- `item_key='kv'`, `item_value='KV Store'`, `category='dashboard'`
- `item_key='sql'`, `item_value='SQL Explorer'`, `category='dashboard'`
- `item_key='sb'`, `item_value='Secondary Brain'`, `category='dashboard'`

## Secondary Brain (Protected)

- `GET /sb` folder/file workspace
- `GET/POST /sb/folder/new`, `GET/POST /sb/folder/<id>/edit`
- `POST /sb/folder/<id>/delete` (move to trash), restore/purge from trash
- `GET/POST /sb/file/new`, `GET/POST /sb/file/<id>/edit`, `GET /sb/file/<id>`
- `POST /sb/file/<id>/autosave` (every 5s + blur in editor)
- `POST /sb/file/<id>/delete` (move to trash), restore/purge from trash
- Bidirectional file links: add/unlink from file editor

Notes:
- Editor preview is client-side markdown.
- Read-mode rendering is server-side markdown with sanitization.
- Folder delete moves full subtree and child files to trash.
- Search in SB is by file title and tags.

Schema SQL is provided in `sql/secondary_brain.sql`.

## SQL Explorer (Protected)

- `GET /sql` - SQL editor page
- `POST /sql/execute` - execute query and render result table

Safety rules:
- Always single-statement only.
- Operation whitelist only: `SELECT, INSERT, UPDATE, DELETE, TRUNCATE, DROP, CREATE`
- Write operations are allowed only if both are true:
  - `APP_ENV=dev`
  - `SQL_UI_WRITE_ENABLED=true` (default is `false`)
- All statements are restricted to tables listed in:
  - `SQL_UI_ALLOWED_TABLES` (comma-separated, required for SQL explorer)
- `SELECT` results are row-limited by `SQL_UI_MAX_ROWS` (default `100`, max `500`)

Unauthenticated access to protected pages redirects to `/login`.

## Key-Value Store CRUD (Protected)

Table expected: `kv_store`

Columns used by app:
- `item_key` (PK, unique)
- `item_value` (`VARCHAR2(500)`, required)
- `additional_info` (`VARCHAR2(4000)`, optional)
- `category` (`VARCHAR2(100)`, optional)
- `is_active` (`CHAR(1)`, default `Y`, values `Y|N`)
- `created_at` (auto)
- `updated_at` (auto)

Pages/endpoints:
- `GET /kv` list + search + category filter + pagination
- `GET/POST /kv/new` create
- `GET /kv/<item_key>` detail
- `GET/POST /kv/<item_key>/edit` update
- `POST /kv/<item_key>/delete` deactivate (soft delete)
- `POST /kv/<item_key>/restore` restore inactive key

Behavior:
- Duplicate key insert shows user-friendly error (`Key already exists`).
- Default list view shows active records; status filter supports `active/inactive/all`.
- Validation limits enforced in app layer:
  - key max 120 chars (safe pattern)
  - value max 500 chars
  - additional info max 4000 chars
