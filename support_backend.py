"""Read-only, single-principal diagnostics for the independent support bot.

The caller supplies the Telegram-authenticated ID, never a model-supplied ID.
No raw account, device identifier, credential or license is returned.
"""
from datetime import datetime, timezone


def _expiry_state(value, now):
    if not value or str(value).lower() == "forever":
        return "active"
    try:
        expiry = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return "expired" if expiry < now else "active"
    except (TypeError, ValueError):
        return "unknown"


def build_support_snapshot(backend, user_id):
    uid = int(user_id)
    if uid <= 0:
        raise ValueError("A Telegram user ID is required")
    now = datetime.now(timezone.utc)
    snapshot = {"checked_at": now.isoformat(), "subscription": {"state": "unknown"},
                "devices": {"state": "unknown"}, "connection": {"state": "unknown"}}
    # Direct child reads deliberately avoid helpers that treat database errors
    # as missing accounts, and avoid license-list scans across all customers.
    try:
        record = backend["approved_ref"]().child(str(uid)).get()
        user = backend["users_ref"]().child(str(uid)).get() or {}
        if uid == int(backend["ADMIN_TELEGRAM_ID"]):
            subscription = {"state": "active", "plan": "admin", "expires_at": "forever"}
        elif not isinstance(record, dict) or not record:
            subscription = {"state": "inactive"}
        else:
            state = _expiry_state(record.get("expires_at"), now)
            if str(record.get("status", "approved")) != "approved":
                state = "inactive"
            if str(user.get("status", "")).lower() in {"blocked", "revoked"}:
                state = "inactive"
            subscription = {"state": state,
                            "plan": str(record.get("plan") or record.get("mode") or "active")[:40],
                            "expires_at": str(record.get("expires_at") or "unspecified")[:40]}
            if record.get("partner_id"):
                code = backend["normalize_partner_code"](record["partner_id"])
                partner = backend["partners_ref"]().child(code).get() or {}
                partner_active = (partner.get("status") == "active"
                                  and _expiry_state(partner.get("expiry_date") or partner.get("expires_at"), now) == "active")
                subscription["partner_active"] = partner_active
                subscription["extension_included"] = bool(partner.get("extension_enabled"))
                subscription["mobile_included"] = bool(partner.get("app_enabled"))
                if not partner_active or not partner.get("bot_enabled", True):
                    subscription["state"] = "inactive"
        snapshot["subscription"] = subscription
    except Exception:
        # Unknown is intentionally distinct from inactive/expired.
        snapshot["subscription"] = {"state": "unknown", "reason": "service_unavailable"}
    try:
        root = backend["copy_telegram_devices_ref"]().child(str(uid)).get() or {}
        mobile = (root.get("clients") or {}).get("mobile") or {}
        snapshot["devices"] = {
            "state": "available",
            "extension_bound": bool(root.get("active_device_id") and root.get("active_device_proof_key")),
            "mobile_bound": bool(mobile.get("active_device_id") and mobile.get("active_device_proof_key")),
        }
    except Exception:
        snapshot["devices"] = {"state": "unknown", "reason": "service_unavailable"}
    try:
        transport = backend["_copy_execution_transport_state_for_user"](uid)
        snapshot["connection"] = {"state": "available",
                                  "extension_online": bool(transport.get("extension")),
                                  "mobile_online": bool(transport.get("mobile_online"))}
    except Exception:
        pass
    # Release metadata is configuration, never a user-supplied download link.
    snapshot["app"] = {"latest_version": str(backend.get("MOBILE_APP_LATEST_VERSION", ""))[:40],
                       "download_url": str(backend.get("MOBILE_APP_RELEASE_URL", ""))[:300]}
    return snapshot
