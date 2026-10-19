import unittest
from datetime import datetime, timedelta, timezone

from support_backend import build_support_snapshot


class Ref:
    def __init__(self, root, path=()):
        self.root, self.path = root, path

    def child(self, key):
        return Ref(self.root, self.path + (key,))

    def get(self):
        self.root["reads"].append(self.path)
        if self.root.get("fail"):
            raise OSError("secret-credential-must-not-leak")
        node = self.root["data"]
        for key in self.path:
            node = (node or {}).get(key)
        return node


class BackendTests(unittest.TestCase):
    def setUp(self):
        self.root = {"reads": [], "data": {
            "approved": {"123": {"status": "approved", "expires_at": "forever", "plan": "month",
                                  "quotex_password": "secret"}, "456": {"status": "approved"}},
            "users": {"123": {"status": "approved"}},
            "devices": {"123": {"active_device_id": "private-device", "active_device_proof_key": "private-proof",
                                  "clients": {"mobile": {"active_device_id": "phone", "active_device_proof_key": "proof"}}}},
            "partners": {},
        }}
        self.backend = {"ADMIN_TELEGRAM_ID": 999,
                        "approved_ref": lambda: Ref(self.root, ("approved",)),
                        "users_ref": lambda: Ref(self.root, ("users",)),
                        "copy_telegram_devices_ref": lambda: Ref(self.root, ("devices",)),
                        "partners_ref": lambda: Ref(self.root, ("partners",)),
                        "normalize_partner_code": lambda value: value,
                        "_copy_execution_transport_state_for_user": lambda uid: {"extension": 1, "mobile_online": False}}

    def test_only_authenticated_principal_read_no_credentials_returned(self):
        result = build_support_snapshot(self.backend, 123)
        self.assertEqual(result["subscription"]["state"], "active")
        self.assertTrue(result["devices"]["mobile_bound"])
        self.assertFalse(result["connection"]["mobile_online"])
        self.assertEqual(self.root["reads"], [("approved", "123"), ("users", "123"), ("devices", "123")])
        self.assertNotIn("private", str(result))
        self.assertNotIn("password", str(result))
        self.assertNotIn("456", str(result))

    def test_database_failure_is_unknown_not_inactive(self):
        self.root["fail"] = True
        result = build_support_snapshot(self.backend, 123)
        self.assertEqual(result["subscription"]["state"], "unknown")
        self.assertEqual(result["devices"]["state"], "unknown")
        self.assertNotIn("credential", str(result))

    def test_expired_and_invalid_expiry_distinguished(self):
        record = self.root["data"]["approved"]["123"]
        record["expires_at"] = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        self.assertEqual(build_support_snapshot(self.backend, 123)["subscription"]["state"], "expired")
        record["expires_at"] = "malformed"
        self.assertEqual(build_support_snapshot(self.backend, 123)["subscription"]["state"], "unknown")

    def test_partner_feature_flags_and_inactive_plan(self):
        self.root["data"]["approved"]["123"]["partner_id"] = "TT-P001"
        self.root["data"]["partners"]["TT-P001"] = {"status": "active", "app_enabled": False, "extension_enabled": True}
        sub = build_support_snapshot(self.backend, 123)["subscription"]
        self.assertEqual(sub["state"], "active")
        self.assertFalse(sub["mobile_included"])
        self.assertTrue(sub["extension_included"])
        self.root["data"]["partners"]["TT-P001"]["bot_enabled"] = False
        self.assertEqual(build_support_snapshot(self.backend, 123)["subscription"]["state"], "inactive")

    def test_missing_account_and_unbound_device(self):
        result = build_support_snapshot(self.backend, 321)
        self.assertEqual(result["subscription"]["state"], "inactive")
        self.assertFalse(result["devices"]["extension_bound"])


if __name__ == "__main__":
    unittest.main()
