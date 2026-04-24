# rover
Explorer

## Project structure

- `app.py` - runtime entrypoint (creates Flask app)
- `app/`
  - `__init__.py` - app factory + global error handler
  - `config.py` - app/session configuration
  - `auth.py` - auth guard, CSRF helpers, session auth utilities
  - `db.py` - Oracle connection + schema validation
  - `routes.py` - HTTP routes
- `templates/`
  - `base.html` - shared production layout shell
  - `login.html` - login page
  - `cities.html` - city listing page
  - `error.html` - generic service error page

## Authentication

This app uses a **single shared login token** with Flask session authentication.

### Required environment variables

- `APP_LOGIN_TOKEN` - secret token you enter on `/login`
- `APP_SECRET_KEY` - Flask session signing key

### Optional environment variable

- `SESSION_COOKIE_SECURE` - `true|false` (default: `false`; set to `true` behind HTTPS)

### Behavior

- Public route: `GET /health`
- Protected routes: `GET /`, `GET /cities`
- Auth routes:
  - `GET/POST /login`
  - `POST /logout`

Unauthenticated access to protected pages redirects to `/login`.
