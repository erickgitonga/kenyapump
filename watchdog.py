"""
watchdog.py

Monitors the KenyaPump process. If the bot dies, sends a Telegram
alert and optionally restarts it.

Run alongside the bot:
    nohup python3 -u watchdog.py > logs_watchdog.log 2>&1 &
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("watchdog")

TELEGRAM_API = "https://api.telegram.org"
BOT_PATTERN = "python3 -u main.py"
CHECK_INTERVAL = 30.0
AUTO_RESTART = True   # set False if you only want alerts
RESTART_COOLDOWN = 60.0  # min seconds between restart attempts


async def send_telegram(text: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat:
        log.warning("Telegram credentials missing — cannot send alert")
        return False

    url = f"{TELEGRAM_API}/bot{token}/sendMessage"
    payload = {"chat_id": chat, "text": text, "parse_mode": "HTML"}

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                return r.status == 200
    except Exception as exc:
        log.warning(f"Telegram send failed: {exc}")
        return False


def bot_is_running() -> bool:
    """True if the bot process is alive."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", BOT_PATTERN],
            capture_output=True,
            text=True,
            timeout=5,
        )
        # Exclude our own watchdog process
        pids = [p for p in result.stdout.strip().split("\n") if p]
        return len(pids) > 0
    except Exception as exc:
        log.warning(f"pgrep failed: {exc}")
        return False


def restart_bot() -> bool:
    """Start the bot in the background. Returns True on success."""
    try:
        log.info("Restarting bot...")
        with open("logs_run.log", "a") as logfile:
            subprocess.Popen(
                ["python3", "-u", "main.py"],
                stdout=logfile,
                stderr=subprocess.STDOUT,
                start_new_session=True,  # detach from watchdog
            )
        return True
    except Exception as exc:
        log.error(f"Restart failed: {exc}")
        return False


async def main() -> None:
    log.info("Watchdog starting")
    log.info(f"Monitoring: {BOT_PATTERN}")
    log.info(f"Check interval: {CHECK_INTERVAL}s")
    log.info(f"Auto restart: {AUTO_RESTART}")

    # Startup notification
    await send_telegram(
        "<b>🐕 Watchdog started</b>\n"
        f"Monitoring: <code>{BOT_PATTERN}</code>\n"
        f"Auto-restart: <b>{AUTO_RESTART}</b>"
    )

    was_alive = True
    last_restart = 0.0

    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        alive = bot_is_running()

        # State change: was alive, now dead
        if was_alive and not alive:
            log.warning("Bot process died")
            await send_telegram(
                "<b>🚨 KenyaPump DIED</b>\n"
                f"Detected at: {time.strftime('%H:%M:%S')}\n"
                f"Auto-restart: {AUTO_RESTART}"
            )

            if AUTO_RESTART:
                now = time.monotonic()
                if now - last_restart >= RESTART_COOLDOWN:
                    ok = restart_bot()
                    last_restart = now
                    if ok:
                        log.info("Bot restarted")
                        await send_telegram(
                            "<b>🔄 Restarted KenyaPump</b>\n"
                            "Waiting to see if it stays up..."
                        )
                    else:
                        await send_telegram(
                            "<b>❌ Restart FAILED</b>\n"
                            "Check logs_run.log manually."
                        )
                else:
                    log.info("Restart cooldown active, skipping")

        # State change: was dead, now alive
        elif not was_alive and alive:
            log.info("Bot is back up")
            await send_telegram("<b>✅ KenyaPump is back up</b>")

        was_alive = alive

        # Heartbeat every 10 checks (5 min) if bot alive
        if alive and int(time.monotonic() / CHECK_INTERVAL) % 10 == 0:
            log.info("Heartbeat — bot alive")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Watchdog stopped")
