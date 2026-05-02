import hashlib
import secrets


def generate_api_key() -> str:
    # URL-safe, high-entropy key string suitable for header transport.
    return f"rvk_{secrets.token_urlsafe(32)}"


def hash_api_key(raw_key: str) -> str:
    text = (raw_key or "").strip()
    if not text:
        raise ValueError("API key is required")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256${digest}"


def verify_api_key(raw_key: str, stored_hash: str) -> bool:
    text = (raw_key or "").strip()
    hashed = (stored_hash or "").strip()
    if not text or not hashed:
        return False

    if hashed.startswith("sha256$"):
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return secrets.compare_digest(f"sha256${digest}", hashed)
    return secrets.compare_digest(text, hashed)


def build_key_reference() -> str:
    return f"ak_{secrets.token_hex(8)}"


def key_preview_parts(raw_key: str) -> tuple[str, str]:
    text = (raw_key or "").strip()
    if not text:
        return "", ""
    return text[:8], text[-4:]
