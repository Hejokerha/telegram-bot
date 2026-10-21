"""A live Quotex display name can legitimately differ from its OTC code."""
import ast
from pathlib import Path
import re
import threading
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"


def load_function(name, namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[name]


class ThreeCandleSymbolAliasTests(unittest.TestCase):
    def setUp(self):
        self.feed = SimpleNamespace(lock=threading.Lock(), instruments={
            "BRLUSD_otc": {"name": "USD/BRL (OTC)", "is_otc": True},
        })
        namespace = {"re": re, "quotex_otc_feed": self.feed}
        load_function("_three_candle_normalize_live_currency_pair", namespace)
        namespace["FIAT_CURRENCY_CODES"] = {"USD", "BRL", "EUR", "JPY"}
        self.normalize = load_function("_copy_server_normalize_pair_contract", namespace)

    def test_verified_live_alias_retains_display_and_execution_symbol(self):
        self.assertEqual(
            self.normalize({"source": "three_candle", "pair": "USD/BRL (OTC)",
                            "platform_symbol": "BRLUSD_otc"}),
            ("USD/BRL (OTC)", "BRLUSD_otc", True),
        )

    def test_unverified_alias_and_other_sources_stay_rejected(self):
        for source, pair in (("bot", "USD/BRL (OTC)"),
                             ("three_candle", "EUR/JPY (OTC)")):
            with self.subTest(source=source, pair=pair), self.assertRaisesRegex(ValueError, "market identity"):
                self.normalize({"source": source, "pair": pair,
                                "platform_symbol": "BRLUSD_otc"})


if __name__ == "__main__":
    unittest.main()
