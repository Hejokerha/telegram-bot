"""Validate only support configuration; never import/start the trading backend."""
import argparse
import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from support_bot import SupportAI, SupportConfig


async def check(live=False):
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env", override=False)
    load_dotenv(root / ".env.support", override=False)
    # Validation should also explain missing values before support is enabled.
    enabled = os.environ.get("SUPPORT_BOT_ENABLED", "false")
    os.environ["SUPPORT_BOT_ENABLED"] = "true"
    try:
        config = SupportConfig.from_env(1582593617)
    except (ValueError, TypeError) as exc:
        print(json.dumps({"ok": False, "reason": str(exc)}))
        return 1
    finally:
        os.environ["SUPPORT_BOT_ENABLED"] = enabled
    print(json.dumps({"ok": True, "enabled": enabled, "model": config.model, "public": config.public,
                      "daily_total": config.daily_total, "daily_per_user": config.daily_per_user}))
    if live:
        ai = SupportAI(config)
        try:
            result = await ai.answer("كيف أبدأ ربط الإضافة؟", [], {"subscription": {"state": "unknown"}})
            print(json.dumps({"live_ai_ok": True, "reply_characters": len(result["reply"]),
                              "needs_human": result["needs_human"]}))
        except Exception as exc:
            print(json.dumps({"live_ai_ok": False, "error_type": type(exc).__name__}))
            return 1
        finally:
            await ai.client.aclose()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-ai", action="store_true")
    raise SystemExit(asyncio.run(check(parser.parse_args().live_ai)))
