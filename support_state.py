"""Durable support-only state on existing Firebase; never writes user accounts."""
from copy import deepcopy
from datetime import datetime, timezone
import secrets
import time

from support_bot import redact


class FirebaseSupportStore:
    # Network operations are dispatched off FastAPI's loop by SupportService.
    blocking = True

    def __init__(self, root_ref):
        self.root_ref = root_ref

    def reserve_request(self, uid, config, now=None):
        stamp = time.time() if now is None else now
        day = datetime.fromtimestamp(stamp, timezone.utc).date().isoformat()
        reason = None

        def reserve(current):
            nonlocal reason
            data = deepcopy(current or {})
            users = data.setdefault("users", {})
            row = users.get(str(uid)) or {}
            reason = None
            if row and stamp - float(row.get("last_at", 0)) < config.cooldown:
                reason = "cooldown"
            elif int(data.get("total", 0)) >= config.daily_total or int(row.get("requests", 0)) >= config.daily_per_user:
                reason = "daily_limit"
            else:
                users[str(uid)] = {"requests": int(row.get("requests", 0)) + 1, "last_at": stamp}
                data["total"] = int(data.get("total", 0)) + 1
            return data

        self.root_ref().child("usage").child(day).transaction(reserve)
        return reason

    def ticket(self, ticket_id):
        return self.root_ref().child("tickets").child(str(ticket_id)).get()

    def open_ticket(self, uid):
        ticket_id = self.root_ref().child("open_by_user").child(str(uid)).get()
        ticket = self.ticket(ticket_id) if ticket_id else None
        return ticket if ticket and ticket.get("status") == "open" else None

    def create_ticket(self, uid, summary):
        existing = self.open_ticket(uid)
        if existing:
            return existing
        ticket_id = secrets.token_hex(5).upper()
        stamp = time.time()
        day = datetime.fromtimestamp(stamp, timezone.utc).date().isoformat()
        record = {"id": ticket_id, "uid": int(uid), "created": stamp,
                  "summary": redact(summary)[:2700], "status": "open", "notified": 0}
        self.root_ref().update({f"tickets/{ticket_id}": record,
                                f"open_by_user/{uid}": ticket_id,
                                f"open_tickets/{ticket_id}": int(uid),
                                f"ticket_days/{day}/{ticket_id}": int(uid)})
        return record

    def mark_notified(self, ticket_id):
        self.root_ref().child("tickets").child(str(ticket_id)).update({"notified": 1})

    def list_open(self):
        ids = self.root_ref().child("open_tickets").get() or {}
        rows = [self.ticket(ticket_id) for ticket_id in ids]
        rows = [row for row in rows if row and row.get("status") == "open"]
        return sorted(rows, key=lambda row: row["created"], reverse=True)[:10]

    def close_ticket(self, ticket_id):
        record = self.ticket(ticket_id)
        if not record:
            return
        self.root_ref().update({f"tickets/{ticket_id}/status": "closed", f"open_tickets/{ticket_id}": None,
                                f"open_by_user/{record['uid']}": None})

    def cleanup(self, now=None):
        cutoff = (time.time() if now is None else now) - 30 * 86400
        cutoff_day = datetime.fromtimestamp(cutoff, timezone.utc).date().isoformat()
        root = self.root_ref()
        changes = {}
        for day in (root.child("ticket_days").get(shallow=True) or {}):
            if day >= cutoff_day:
                continue
            for ticket_id, uid in (root.child("ticket_days").child(day).get() or {}).items():
                changes[f"tickets/{ticket_id}"] = None
                changes[f"open_tickets/{ticket_id}"] = None
                if root.child("open_by_user").child(str(uid)).get() == ticket_id:
                    changes[f"open_by_user/{uid}"] = None
            changes[f"ticket_days/{day}"] = None
        for day in (root.child("usage").get(shallow=True) or {}):
            if day < cutoff_day:
                changes[f"usage/{day}"] = None
        if changes:
            root.update(changes)

    def close(self):
        pass
