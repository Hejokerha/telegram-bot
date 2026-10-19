from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import unittest

from support_bot import SupportConfig
from support_state import FirebaseSupportStore


class MemoryRef:
    def __init__(self, state, path=()):
        self.state, self.path = state, path

    def child(self, name):
        return MemoryRef(self.state, self.path + (str(name),))

    def get(self, shallow=False):
        node = self.state
        for key in self.path:
            node = (node or {}).get(key)
        if shallow and node:
            return {key: True for key in node}
        return deepcopy(node)

    def set(self, value):
        node = self.state
        for key in self.path[:-1]:
            node = node.setdefault(key, {})
        if value is None:
            node.pop(self.path[-1], None)
        else:
            node[self.path[-1]] = deepcopy(value)

    def update(self, values):
        for key, value in values.items():
            target = self
            for segment in key.split("/"):
                target = target.child(segment)
            target.set(value)

    def transaction(self, callback):
        # Simulate SDK callback retry: only the second candidate is committed.
        callback(self.get())
        candidate = callback(self.get())
        self.set(candidate)
        return candidate


class FirebaseStateTests(unittest.TestCase):
    def setUp(self):
        self.data = {"users": {"123": {"private": "untouched"}}}
        self.ref = MemoryRef(self.data).child("system").child("support_bot")
        self.store = FirebaseSupportStore(lambda: self.ref)
        self.cfg = SupportConfig("unused", "unused", 999, frozenset({123}), daily_total=3, daily_per_user=2)

    def test_global_cap_survives_new_store_and_callback_retry(self):
        self.assertIsNone(self.store.reserve_request(123, self.cfg, 1800000000))
        self.assertEqual(self.store.reserve_request(123, self.cfg, 1800000001), "cooldown")
        self.assertIsNone(self.store.reserve_request(123, self.cfg, 1800000011))
        self.assertEqual(self.store.reserve_request(123, self.cfg, 1800000022), "daily_limit")
        other = FirebaseSupportStore(lambda: self.ref)
        self.assertIsNone(other.reserve_request(456, self.cfg, 1800000033))
        self.assertEqual(other.reserve_request(789, self.cfg, 1800000044), "daily_limit")
        self.assertEqual(self.data["users"]["123"], {"private": "untouched"})

    def test_ticket_delivery_close_and_redaction_survive_restart(self):
        ticket = self.store.create_ticket(123, "token=private-secret")
        self.assertNotIn("private-secret", ticket["summary"])
        self.assertEqual(self.store.create_ticket(123, "duplicate")["id"], ticket["id"])
        self.store.mark_notified(ticket["id"])
        other = FirebaseSupportStore(lambda: self.ref)
        self.assertTrue(other.open_ticket(123)["notified"])
        self.assertEqual(other.list_open()[0]["id"], ticket["id"])
        self.assertIsNone(other.open_ticket(456))
        other.close_ticket(ticket["id"])
        self.assertIsNone(other.open_ticket(123))
        self.assertEqual(other.ticket(ticket["id"])["status"], "closed")
        self.assertEqual(other.list_open(), [])

    def test_retention_prunes_only_support_data(self):
        self.ref.update({"tickets/old": {"id": "old", "uid": 123}, "open_by_user/123": "new",
                         "open_tickets/old": 123, "ticket_days/2020-01-01/old": 123,
                         "usage/2020-01-01": {"total": 2}})
        self.store.cleanup(now=1800000000)
        self.assertIsNone(self.store.ticket("old"))
        self.assertEqual(self.ref.child("open_by_user").child("123").get(), "new")
        self.assertEqual(self.data["users"]["123"], {"private": "untouched"})


if __name__ == "__main__":
    unittest.main()
