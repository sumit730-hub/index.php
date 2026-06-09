#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║         🔐 PHANTOM ESCROW BOT v5.0 — UNKNOWNBABU 🔐             ║
╠══════════════════════════════════════════════════════════════════╣
║  ✅ /start command — DM + Group dono mein kaam karta hai        ║
║  ✅ QR API — Dynamic QR generate hota hai (qrserver.com)        ║
║  ✅ Seller → File upload → Admin forward → Buyer release        ║
║  ✅ Smart UPI auto-detection (regex)                             ║
║  ✅ Deal expiry scheduler (background job)                       ║
║  ✅ FSM-based state machine (no invalid transitions)             ║
║  ✅ Seller fee deduction support                                 ║
║  ✅ Multi-deal per group (sequential queue)                      ║
║  ✅ Inline admin panel with real-time updates                    ║
║  ✅ Rate-limited broadcast                                       ║
║  ✅ Full transaction log with timestamps                         ║
║  ✅ Graceful shutdown + data persistence                         ║
║  ✅ Photo/Video/File/Document upload support                     ║
║  ✅ All seller media auto-forwarded to ADMIN                     ║
║  ✅ Media vault per deal (multiple files supported)              ║
║  ✅ Buyer gets media only AFTER payment confirmed                ║
║  ✅ Admin approve/reject individual files                        ║
║  ✅ /myfiles — seller uploaded files dekhe                       ║
║  ✅ /deliver — seller explicitly deliver command                 ║
║  ✅ Admin /sendfiles — buyer ko files release karo               ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL:
  pip install "python-telegram-bot[job-queue]>=20.0" aiofiles aiohttp

RUN:
  python escrow_bot_v5.py
"""

# ─────────────────────────────────────────────────────────────────
#  DEPENDENCIES
# ─────────────────────────────────────────────────────────────────
import os
import re
import json
import uuid
import logging
import asyncio
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional
from enum import Enum

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, Message
)
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler,
    MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.constants import ParseMode
from telegram.error import TelegramError

# ══════════════════════════════════════════════════════════════════
#  🔧  CONFIGURATION — APNI DETAILS YAHAN BHARO
# ══════════════════════════════════════════════════════════════════
BOT_TOKEN = os.getenv("BOT_TOKEN")  # @BotFather se lo
ADMIN_ID = int(os.getenv("ADMIN_ID"))          # Tumhara Telegram numeric ID
ADMIN_UPI = os.getenv("ADMIN_UPI")      # Tumhara UPI ID
QR_FILE         = "admin_qr.jpg"      # Local QR image (optional — agar file nahi to API use hogi)
DATA_FILE       = "phantom_data.json" # Database JSON file
DEAL_EXPIRE_HR  = 24                  # Deal expiry in hours
SERVICE_FEE_PCT = 5                   # % fee deducted from amount (0 = free)
LOG_LEVEL       = logging.INFO

# File upload settings
MAX_FILES_PER_DEAL = 10
ALLOWED_FILE_TYPES = {
    "photo":     True,
    "video":     True,
    "document":  True,
    "audio":     True,
    "animation": True,
}

# QR API — agar local QR file nahi mili to yeh use hogi
# qrserver.com FREE hai, koi API key nahi chahiye
QR_API_URL = "https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={data}"
# ══════════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=LOG_LEVEL
)
log = logging.getLogger("PhantomEscrow")

UPI_REGEX = re.compile(
    r"[a-zA-Z0-9.\-_+]+@[a-zA-Z0-9]+(?:\.[a-zA-Z]{2,})*",
    re.IGNORECASE
)

# ══════════════════════════════════════════════════════════════════
#  STATE MACHINE
# ══════════════════════════════════════════════════════════════════
class DealStatus(str, Enum):
    PENDING   = "pending"
    PAID      = "paid"
    DELIVERED = "delivered"
    COMPLETE  = "complete"
    DISPUTE   = "dispute"
    CANCELLED = "cancelled"
    EXPIRED   = "expired"

VALID_TRANSITIONS = {
    DealStatus.PENDING:   {DealStatus.PAID, DealStatus.CANCELLED, DealStatus.EXPIRED, DealStatus.DISPUTE},
    DealStatus.PAID:      {DealStatus.DELIVERED, DealStatus.DISPUTE, DealStatus.CANCELLED},
    DealStatus.DELIVERED: {DealStatus.COMPLETE, DealStatus.DISPUTE},
    DealStatus.COMPLETE:  set(),
    DealStatus.DISPUTE:   {DealStatus.COMPLETE, DealStatus.CANCELLED},
    DealStatus.CANCELLED: set(),
    DealStatus.EXPIRED:   set(),
}

STATUS_EMOJI = {
    DealStatus.PENDING:   "⏳",
    DealStatus.PAID:      "💳",
    DealStatus.DELIVERED: "📦",
    DealStatus.COMPLETE:  "🎉",
    DealStatus.DISPUTE:   "⚠️",
    DealStatus.CANCELLED: "❌",
    DealStatus.EXPIRED:   "💀",
}

STATUS_LABEL = {
    DealStatus.PENDING:   "Payment Awaited",
    DealStatus.PAID:      "Payment in Escrow",
    DealStatus.DELIVERED: "Delivered — Release Pending",
    DealStatus.COMPLETE:  "Completed ✓",
    DealStatus.DISPUTE:   "Under Dispute",
    DealStatus.CANCELLED: "Cancelled",
    DealStatus.EXPIRED:   "Expired",
}

FILE_TYPE_EMOJI = {
    "photo":     "🖼️",
    "video":     "🎬",
    "document":  "📄",
    "audio":     "🎵",
    "animation": "🎞️",
}

# ══════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════
class DB:
    _DEFAULTS = {
        "deals":  {},
        "groups": {},
        "txns":   [],
        "files":  {},
        "stats":  {"total": 0, "completed": 0, "volume": 0.0, "disputed": 0},
    }

    def __init__(self, path: str):
        self.path  = path
        self._data = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    for k, v in self._DEFAULTS.items():
                        loaded.setdefault(k, v if not isinstance(v, dict) else v.copy())
                    return loaded
            except Exception as e:
                log.error(f"DB load error: {e}. Using defaults.")
        return {k: (v.copy() if isinstance(v, dict) else list(v))
        for k, v in self._DEFAULTS.items()}

    def save(self):
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.error(f"DB save error: {e}")

    # ── DEALS ─────────────────────────────────────────────────────
    def get_deal(self, did: str) -> Optional[dict]:
        return self._data["deals"].get(did)

    def set_deal(self, did: str, d: dict):
        self._data["deals"][did] = d
        self.save()

    def transition(self, did: str, new_status: DealStatus) -> bool:
        deal = self.get_deal(did)
        if not deal:
            return False
        cur = DealStatus(deal["status"])
        if new_status not in VALID_TRANSITIONS[cur]:
            return False
        deal["status"] = new_status.value
        deal[f"{new_status.value}_at"] = _now()
        self.set_deal(did, deal)
        if new_status == DealStatus.COMPLETE:
            self._data["stats"]["completed"] += 1
            self._data["stats"]["volume"]    += deal.get("amount", 0)
            self.save()
        elif new_status == DealStatus.DISPUTE:
            self._data["stats"]["disputed"] += 1
            self.save()
        return True

    def active_deals(self) -> list:
        return [d for d in self._data["deals"].values()
                if d["status"] not in (DealStatus.COMPLETE, DealStatus.CANCELLED, DealStatus.EXPIRED)]

    # ── GROUPS ────────────────────────────────────────────────────
    def get_group(self, gid: int) -> Optional[dict]:
        return self._data["groups"].get(str(gid))

    def set_group(self, gid: int, g: dict):
        self._data["groups"][str(gid)] = g
        self.save()

    # ── TXNS ──────────────────────────────────────────────────────
    def add_txn(self, d: dict):
        self._data["txns"].append(d)
        self._data["stats"]["total"] += 1
        self.save()

    # ── FILES ─────────────────────────────────────────────────────
    def add_file(self, did: str, file_entry: dict):
        if did not in self._data["files"]:
            self._data["files"][did] = []
        self._data["files"][did].append(file_entry)
        self.save()

    def get_files(self, did: str) -> list:
        return self._data["files"].get(did, [])

    def count_files(self, did: str) -> int:
        return len(self._data["files"].get(did, []))

    def approve_file(self, did: str, file_idx: int, approved: bool):
        files = self._data["files"].get(did, [])
        if 0 <= file_idx < len(files):
            files[file_idx]["admin_approved"] = approved
            self._data["files"][did] = files
            self.save()
            return True
        return False

    def get_approved_files(self, did: str) -> list:
        return [f for f in self.get_files(did) if f.get("admin_approved") is not False]

    # ── STATS ─────────────────────────────────────────────────────
    @property
    def stats(self) -> dict:
        return self._data["stats"]

    def all_user_ids(self) -> set:
        ids = set()
        for d in self._data["deals"].values():
            for k in ("buyer_id", "seller_id"):
                if d.get(k):
                    ids.add(d[k])
        return ids


db = DB(DATA_FILE)

# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════
def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def _expire_at() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=DEAL_EXPIRE_HR)).strftime("%Y-%m-%d %H:%M UTC")

def _deal_id() -> str:
    return "ESC" + uuid.uuid4().hex[:8].upper()

def _txn_id() -> str:
    return "TXN" + uuid.uuid4().hex[:10].upper()

def _file_id() -> str:
    return "FID" + uuid.uuid4().hex[:8].upper()

def _fee_amt(amount: float) -> tuple:
    fee = round(amount * SERVICE_FEE_PCT / 100, 2)
    return fee, round(amount - fee, 2)

def _find_upi(text: str) -> Optional[str]:
    m = UPI_REGEX.search(text)
    return m.group(0) if m else None

def _qr_api_url(upi: str, amount: float, note: str) -> str:
    """Dynamic QR URL generate karo UPI ke liye."""
    upi_string = f"upi://pay?pa={upi}&am={amount:.2f}&tn={note}&cu=INR"
    return QR_API_URL.format(data=urllib.parse.quote(upi_string))

async def _safe_send(bot, chat_id, text, **kwargs):
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramError as e:
        log.warning(f"send failed to {chat_id}: {e}")

def _get_file_type(msg: Message) -> Optional[str]:
    if msg.photo:      return "photo"
    if msg.video:      return "video"
    if msg.document:   return "document"
    if msg.audio:      return "audio"
    if msg.animation:  return "animation"
    return None

def _get_file_id_from_msg(msg: Message) -> Optional[str]:
    if msg.photo:     return msg.photo[-1].file_id
    if msg.video:     return msg.video.file_id
    if msg.document:  return msg.document.file_id
    if msg.audio:     return msg.audio.file_id
    if msg.animation: return msg.animation.file_id
    return None

def _get_file_name(msg: Message) -> str:
    if msg.document and msg.document.file_name:
        return msg.document.file_name
    if msg.audio and msg.audio.file_name:
        return msg.audio.file_name
    if msg.video:     return "video.mp4"
    if msg.photo:     return "photo.jpg"
    if msg.animation: return "animation.gif"
    return "file"

# ══════════════════════════════════════════════════════════════════
#  KEYBOARDS
# ══════════════════════════════════════════════════════════════════
def kb_deal_actions(deal_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💸 Maine Pay Kar Diya", callback_data=f"paid:{deal_id}"),
        ],
        [
            InlineKeyboardButton("📊 Status Dekho",  callback_data=f"status:{deal_id}"),
            InlineKeyboardButton("❌ Cancel Deal",   callback_data=f"cancel:{deal_id}"),
        ],
    ])

def kb_admin(deal_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Payment Confirm",    callback_data=f"adm_confirm:{deal_id}"),
            InlineKeyboardButton("💸 Seller ko Release",  callback_data=f"adm_release:{deal_id}"),
        ],
        [
            InlineKeyboardButton("🚫 Cancel Deal",        callback_data=f"adm_cancel:{deal_id}"),
            InlineKeyboardButton("📋 Deal Details",       callback_data=f"adm_details:{deal_id}"),
        ],
        [
            InlineKeyboardButton("📂 Files Dekho",        callback_data=f"adm_files:{deal_id}"),
            InlineKeyboardButton("📤 Files → Buyer",      callback_data=f"adm_sendfiles:{deal_id}"),
        ],
    ])

def kb_file_admin(deal_id: str, file_idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve",  callback_data=f"file_approve:{deal_id}:{file_idx}"),
            InlineKeyboardButton("❌ Reject",   callback_data=f"file_reject:{deal_id}:{file_idx}"),
        ],
    ])

def kb_group_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Status",  callback_data="menu:status"),
            InlineKeyboardButton("⚠️ Dispute", callback_data="menu:dispute"),
        ],
        [InlineKeyboardButton("❓ Help",       callback_data="menu:help")],
    ])

# ══════════════════════════════════════════════════════════════════
#  ✅ START COMMAND — DM + Group dono mein kaam karta hai
# ══════════════════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u    = update.effective_user
    chat = update.effective_chat

    # Group mein /start
    if chat.type in ("group", "supergroup"):
        g = db.get_group(chat.id)
        if not g:
            await update.message.reply_text(
                f"👋 *Phantom Escrow v5 yahan hai!*\n\n"
                f"Group setup karne ke liye:\n"
                f"1️⃣ `/setup` — Bot initialize karo\n"
                f"2️⃣ `/buyer` — Buyer register karo\n"
                f"3️⃣ `/seller` — Seller register karo\n"
                f"4️⃣ `/deal [product] [amount]` — Deal banao\n\n"
                f"📩 Private commands ke liye bot ka DM kholo.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            deal = db.get_deal(g.get("deal_id", "")) if g.get("deal_id") else None
            status_line = ""
            if deal:
                st = DealStatus(deal["status"])
                status_line = f"\n📌 Active deal: `{deal['deal_id']}` — {STATUS_EMOJI[st]} {STATUS_LABEL[st]}"
            await update.message.reply_text(
                f"🔐 *Phantom Escrow v5*\n\n"
                f"Group: {chat.title}\n"
                f"🛒 Buyer: {'✅ Set' if g.get('buyer_id') else '❌ Not set'}\n"
                f"📦 Seller: {'✅ Set' if g.get('seller_id') else '❌ Not set'}"
                f"{status_line}\n\n"
                f"`/help` se saare commands dekho.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_group_menu()
            )
        return

    # Private DM mein /start — Achha welcome message
    is_admin = u.id == ADMIN_ID
    text = (
        f"🔐 *PHANTOM ESCROW v5* 🔐\n\n"
        f"Namaste *{u.first_name}*! Secure buyer-seller escrow bot.\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"*📌 Kaise kaam karta hai:*\n\n"
        f"1️⃣ Ek group banao, mujhe admin banao\n"
        f"2️⃣ Group mein `/setup` karo\n"
        f"3️⃣ `/buyer` — buyer register karo\n"
        f"4️⃣ `/seller` — seller register karo\n"
        f"5️⃣ `/deal [product] [amount]` — deal banao (seller)\n"
        f"6️⃣ Buyer admin UPI pe payment kare + QR scan kare\n"
        f"7️⃣ Admin payment confirm kare\n"
        f"8️⃣ Seller group mein files/media upload kare\n"
        f"9️⃣ Seller `/deliver` kare\n"
        f"🔟 Admin files review karke buyer ko bheje\n"
        f"1️⃣1️⃣ Buyer `/release` kare → Seller UPI bheje → Admin `/done`\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆕 *Seller ab photo/video/file/doc upload kar sakta hai!*\n"
        f"📱 *QR auto-generate hota hai deal creation pe!*\n\n"
        f"Safe trading with @Cyber_X_Helper 🔥"
    )
    if is_admin:
        text += "\n\n👑 *Admin panel:* `/admin`"

    print("TEXT LENGTH =", len(text))
    print(text)

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN
    )

# ══════════════════════════════════════════════════════════════════
#  HELP COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u        = update.effective_user
    is_admin = u.id == ADMIN_ID
    text = (
        "📖 *SAARE COMMANDS*\n\n"
        "*🏠 Group Commands:*\n"
        "`/setup` — Bot initialize karo\n"
        "`/buyer` — Buyer ban jao\n"
        "`/seller` — Seller ban jao\n"
        "`/deal [product] [amount]` — Deal banao (seller)\n"
        "`/deliver` — Files deliver karo (seller)\n"
        "`/myfiles` — Uploaded files dekho (seller)\n"
        "`/release` — Product theek hai, payment release karo (buyer)\n"
        "`/status` — Deal status dekho\n"
        "`/dispute [reason]` — Complaint karo\n"
        "`/cancel` — Deal cancel karo\n\n"
        "*📩 Private Commands:*\n"
        "`/start` — Welcome message\n"
        "`/history` — Apni deals dekho"
    )
    if is_admin:
        text += (
            "\n\n*👑 Admin Commands:*\n"
            "`/admin` — Admin panel\n"
            "`/confirm [id]` — Payment confirm karo\n"
            "`/sendfiles [id]` — Files buyer ko bhejo\n"
            "`/done [id]` — Deal complete karo\n"
            "`/refund [id]` — Refund karo\n"
            "`/stats` — Statistics\n"
            "`/deals` — Active deals\n"
            "`/broadcast [msg]` — Sabko message bhejo"
        )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ══════════════════════════════════════════════════════════════════
#  SETUP COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_setup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    if db.get_group(chat.id):
        await update.message.reply_text(
            "⚠️ Yeh group pehle se setup hai!\n\n"
            "`/status` se current status dekho.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    g = {
        "group_id":  chat.id,
        "buyer_id":  None,
        "seller_id": None,
        "deal_id":   None,
        "status":    "idle",
        "setup_at":  _now(),
    }
    db.set_group(chat.id, g)
    await update.message.reply_text(
        f"✅ *Group Setup Complete!*\n\n"
        f"Group: *{chat.title}*\n\n"
        f"Ab karo:\n"
        f"👤 `/buyer` — buyer register karo\n"
        f"📦 `/seller` — seller register karo",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  BUYER / SELLER REGISTRATION
# ══════════════════════════════════════════════════════════════════
async def cmd_buyer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g:
        await update.message.reply_text("❌ Pehle `/setup` karo!", parse_mode=ParseMode.MARKDOWN)
        return
    if g["buyer_id"]:
        if g["buyer_id"] == user.id:
            await update.message.reply_text("✅ Tum pehle se Buyer ho!")
        else:
            await update.message.reply_text("❌ Buyer pehle se set hai!")
        return
    if g["seller_id"] == user.id:
        await update.message.reply_text("❌ Tum seller ho, buyer nahi ban sakte!")
        return
    g["buyer_id"] = user.id
    db.set_group(chat.id, g)
    await update.message.reply_text(
        f"🛒 *{user.first_name}* ab BUYER hai!\n\nSeller: `/seller`",
        parse_mode=ParseMode.MARKDOWN
    )
    await _check_roles(update, ctx, chat.id)


async def cmd_seller(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g:
        await update.message.reply_text("❌ Pehle `/setup` karo!", parse_mode=ParseMode.MARKDOWN)
        return
    if g["seller_id"]:
        if g["seller_id"] == user.id:
            await update.message.reply_text("✅ Tum pehle se Seller ho!")
        else:
            await update.message.reply_text("❌ Seller pehle se set hai!")
        return
    if g["buyer_id"] == user.id:
        await update.message.reply_text("❌ Tum buyer ho, seller nahi ban sakte!")
        return
    g["seller_id"] = user.id
    db.set_group(chat.id, g)
    await update.message.reply_text(
        f"📦 *{user.first_name}* ab SELLER hai!\n\nBuyer: `/buyer`",
        parse_mode=ParseMode.MARKDOWN
    )
    await _check_roles(update, ctx, chat.id)


async def _check_roles(update: Update, ctx: ContextTypes.DEFAULT_TYPE, gid: int):
    g = db.get_group(gid)
    if not (g and g["buyer_id"] and g["seller_id"]):
        return
    try:
        buyer  = await ctx.bot.get_chat(g["buyer_id"])
        seller = await ctx.bot.get_chat(g["seller_id"])
    except Exception:
        buyer  = type("U", (), {"first_name": "Buyer"})()
        seller = type("U", (), {"first_name": "Seller"})()
    g["status"] = "ready"
    db.set_group(gid, g)
    await ctx.bot.send_message(
        gid,
        f"🎯 *Dono Ready Hain!*\n\n"
        f"🛒 Buyer: *{buyer.first_name}*\n"
        f"📦 Seller: *{seller.first_name}*\n\n"
        f"Seller deal banao:\n"
        f"`/deal [product] [amount]`\n\n"
        f"*Example:* `/deal Netflix_Premium 299`",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  DEAL CREATION — QR API INTEGRATED
# ══════════════════════════════════════════════════════════════════
async def cmd_deal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g:
        await update.message.reply_text("❌ Pehle `/setup`!", parse_mode=ParseMode.MARKDOWN)
        return
    if g["seller_id"] != user.id:
        await update.message.reply_text("❌ Sirf seller deal bana sakta hai!")
        return
    if not g["buyer_id"]:
        await update.message.reply_text("❌ Pehle buyer set karo! `/buyer`", parse_mode=ParseMode.MARKDOWN)
        return
    if g.get("deal_id"):
        existing = db.get_deal(g["deal_id"])
        if existing and existing["status"] in (DealStatus.PENDING, DealStatus.PAID, DealStatus.DELIVERED):
            await update.message.reply_text(
                f"❌ Ek active deal pehle se hai: `{g['deal_id']}`\n"
                f"Pehle usse complete/cancel karo.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

    if len(ctx.args) < 2:
        await update.message.reply_text(
            "❌ *Usage:* `/deal [product] [amount]`\n\n"
            "*Example:* `/deal Netflix_Premium 299`\n"
            "*Example:* `/deal Instagram_Account 500`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    product = " ".join(ctx.args[:-1]).replace("_", " ")
    try:
        amount = float(ctx.args[-1])
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Valid amount dalo! (e.g. 299)")
        return

    did         = _deal_id()
    fee, payout = _fee_amt(amount)
    deal = {
        "deal_id":            did,
        "group_id":           chat.id,
        "product":            product,
        "amount":             amount,
        "fee":                fee,
        "payout":             payout,
        "seller_id":          g["seller_id"],
        "buyer_id":           g["buyer_id"],
        "status":             DealStatus.PENDING.value,
        "created_at":         _now(),
        "expires_at":         _expire_at(),
        "seller_upi":         None,
        "txn_id":             None,
        "dispute_reason":     None,
        "files_sent_to_buyer": False,
    }
    db.set_deal(did, deal)
    db.add_txn({
        "txn_id":     _txn_id(),
        "deal_id":    did,
        "product":    product,
        "amount":     amount,
        "buyer_id":   g["buyer_id"],
        "seller_id":  g["seller_id"],
        "status":     "created",
        "created_at": _now(),
    })
    g["deal_id"] = did
    db.set_group(chat.id, g)

    try:
        buyer  = await ctx.bot.get_chat(g["buyer_id"])
        seller = await ctx.bot.get_chat(g["seller_id"])
        buyer_name  = buyer.first_name
        seller_name = seller.first_name
    except Exception:
        buyer_name  = "Buyer"
        seller_name = "Seller"

    fee_line = f"\n_ℹ️ Service fee: {SERVICE_FEE_PCT}% (₹{fee:.2f}) | Payout: ₹{payout:.2f}_" if SERVICE_FEE_PCT > 0 else ""

    text = (
        f"📋 *DEAL CREATED!*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 `{did}`\n"
        f"📦 *Product:* {product}\n"
        f"💰 *Amount:* ₹{amount:,.2f}\n"
        f"🛒 *Buyer:* {buyer_name}\n"
        f"📦 *Seller:* {seller_name}\n"
        f"⏰ *Expires:* {deal['expires_at']}\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"*💳 Buyer, payment karo:*\n"
        f"UPI ID: `{ADMIN_UPI}`\n"
        f"Amount: `₹{amount:,.2f}`\n"
        f"Note mein likho: `{did}`\n\n"
        f"📱 *Ya neeche QR scan karo*\n"
        f"Payment ke baad button dabaao 👇"
        f"{fee_line}"
    )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_deal_actions(did)
    )

    # ── QR Code bhejo ─────────────────────────────────────────────
    await _send_qr(ctx.bot, chat.id, amount, did)

    # ── Admin ko notify karo ──────────────────────────────────────
    await _safe_send(
        ctx.bot, ADMIN_ID,
        f"🔔 *NEW DEAL*\n\n"
        f"🆔 `{did}`\n"
        f"📦 {product}\n"
        f"💰 ₹{amount:,.2f}\n"
        f"🛒 {buyer_name} → 📦 {seller_name}\n"
        f"⏰ {deal['created_at']}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_admin(did)
    )

    # ── Deal expiry schedule karo ──────────────────────────────────
    ctx.job_queue.run_once(
        _expire_deal_job,
        when=timedelta(hours=DEAL_EXPIRE_HR),
        data={"deal_id": did, "group_id": chat.id},
        name=f"expire_{did}"
    )


async def _send_qr(bot, chat_id: int, amount: float, deal_id: str):
    """
    QR code bhejo — pehle local file try karo, phir API se generate karo.
    """
    # 1) Local QR file check
    if os.path.exists(QR_FILE):
        try:
            with open(QR_FILE, "rb") as f:
                await bot.send_photo(
                    chat_id, f,
                    caption=(
                        f"📱 *QR Scan karke pay karo*\n\n"
                        f"💰 Amount: `₹{amount:,.2f}`\n"
                        f"🆔 Deal: `{deal_id}`\n"
                        f"📝 Note mein Deal ID zaroor likho!"
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
            log.info(f"Local QR sent for {deal_id}")
            return
        except Exception as e:
            log.warning(f"Local QR send failed: {e}, trying API...")

    # 2) QR API se dynamic QR generate karo
    try:
        qr_url = _qr_api_url(ADMIN_UPI, amount, deal_id)
        await bot.send_photo(
            chat_id,
            qr_url,
            caption=(
                f"📱 *QR Scan karke pay karo*\n\n"
                f"💰 Amount: `₹{amount:,.2f}`\n"
                f"📲 UPI: `{ADMIN_UPI}`\n"
                f"🆔 Deal: `{deal_id}`\n"
                f"📝 Note mein Deal ID zaroor likho!"
            ),
            parse_mode=ParseMode.MARKDOWN
        )
        log.info(f"API QR sent for {deal_id}")
    except Exception as e:
        # Fallback: sirf UPI text bhejo
        log.warning(f"QR API failed: {e}")
        await _safe_send(
            bot, chat_id,
            f"📲 *UPI Payment Details*\n\n"
            f"💳 UPI: `{ADMIN_UPI}`\n"
            f"💰 Amount: `₹{amount:,.2f}`\n"
            f"📝 Note: `{deal_id}`",
            parse_mode=ParseMode.MARKDOWN
        )


# ══════════════════════════════════════════════════════════════════
#  DEAL EXPIRY JOB
# ══════════════════════════════════════════════════════════════════
async def _expire_deal_job(ctx: ContextTypes.DEFAULT_TYPE):
    data = ctx.job.data
    did  = data["deal_id"]
    gid  = data["group_id"]
    deal = db.get_deal(did)
    if not deal or deal["status"] != DealStatus.PENDING.value:
        return
    if db.transition(did, DealStatus.EXPIRED):
        await _safe_send(
            ctx.bot, gid,
            f"💀 *Deal Expired!*\n\n"
            f"🆔 `{did}` — {DEAL_EXPIRE_HR} ghante mein payment nahi aayi.\n"
            f"Naya deal banane ke liye `/deal` karo.",
            parse_mode=ParseMode.MARKDOWN
        )
        await _safe_send(
            ctx.bot, ADMIN_ID,
            f"💀 Deal `{did}` auto-expire ho gaya.",
            parse_mode=ParseMode.MARKDOWN
        )


# ══════════════════════════════════════════════════════════════════
#  📁 FILE UPLOAD HANDLER — Seller group mein files bhejta hai
# ══════════════════════════════════════════════════════════════════
async def handle_media(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Flow:
    1. Seller group mein file bheja
    2. DB mein save karo
    3. Admin ko forward karo (with Approve/Reject buttons)
    4. Seller ko confirm karo
    5. Buyer ko ABHI MAT bhejo (jab admin /sendfiles kare tab)
    """
    chat = update.effective_chat
    user = update.effective_user
    msg  = update.message

    if chat.type not in ("group", "supergroup"):
        return

    g = db.get_group(chat.id)
    if not g or not g.get("deal_id"):
        return

    deal = db.get_deal(g["deal_id"])
    if not deal:
        return

    # Sirf seller upload kar sakta hai
    if user.id != deal["seller_id"]:
        return

    # Deal active hona chahiye
    if deal["status"] not in (DealStatus.PENDING.value, DealStatus.PAID.value, DealStatus.DELIVERED.value):
        await msg.reply_text(
            f"❌ Is stage pe file upload nahi ho sakti.\n"
            f"Deal status: `{deal['status']}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    file_type = _get_file_type(msg)
    if not file_type:
        return

    if not ALLOWED_FILE_TYPES.get(file_type, False):
        await msg.reply_text(f"❌ {file_type} allowed nahi!")
        return

    current_count = db.count_files(deal["deal_id"])
    if current_count >= MAX_FILES_PER_DEAL:
        await msg.reply_text(
            f"❌ Max {MAX_FILES_PER_DEAL} files allowed hain per deal!\n"
            f"Abhi {current_count} files upload ho chuki hain."
        )
        return

    tg_file_id = _get_file_id_from_msg(msg)
    file_name  = _get_file_name(msg)
    caption    = msg.caption or ""
    local_fid  = _file_id()

    file_entry = {
        "local_fid":      local_fid,
        "tg_file_id":     tg_file_id,
        "file_type":      file_type,
        "file_name":      file_name,
        "caption":        caption,
        "uploaded_by":    user.id,
        "uploaded_at":    _now(),
        "admin_approved": None,   # None=pending, True=approved, False=rejected
        "admin_msg_id":   None,
        "deal_id":        deal["deal_id"],
    }
    db.add_file(deal["deal_id"], file_entry)
    file_idx = db.count_files(deal["deal_id"]) - 1

    emoji = FILE_TYPE_EMOJI.get(file_type, "📁")

    # ── Seller ko confirm ─────────────────────────────────────────
    await msg.reply_text(
        f"{emoji} *File Upload Ho Gayi!*\n\n"
        f"🆔 File ID: `{local_fid}`\n"
        f"📁 Type: `{file_type}`\n"
        f"📄 Name: `{file_name}`\n"
        f"🆔 Deal: `{deal['deal_id']}`\n"
        f"📊 Files uploaded: {file_idx + 1}/{MAX_FILES_PER_DEAL}\n\n"
        f"_Admin review karega. Approved hone ke baad buyer ko milega._\n"
        f"Sab files upload ho jayen toh `/deliver` karo.",
        parse_mode=ParseMode.MARKDOWN
    )

    # ── Admin ko forward karo ─────────────────────────────────────
    try:
        admin_caption = (
            f"📥 *SELLER FILE UPLOAD*\n\n"
            f"🆔 Deal: `{deal['deal_id']}`\n"
            f"📦 Product: {deal['product']}\n"
            f"💰 ₹{deal['amount']:,.2f}\n"
            f"👤 Seller: {user.first_name} (`{user.id}`)\n"
            f"📁 Type: `{file_type}` | Name: `{file_name}`\n"
            f"🔢 File #{file_idx + 1}/{MAX_FILES_PER_DEAL}\n\n"
            f"⬇️ Approve ya Reject karo:"
        )

        admin_msg = await _forward_file_to_admin(
            ctx.bot, msg, file_type, tg_file_id, admin_caption
        )

        if admin_msg:
            files = db.get_files(deal["deal_id"])
            if file_idx < len(files):
                files[file_idx]["admin_msg_id"] = admin_msg.message_id
                db._data["files"][deal["deal_id"]] = files
                db.save()

        # Approve/Reject buttons bhejo
        await _safe_send(
            ctx.bot, ADMIN_ID,
            f"⬆️ *Upar wali file ke liye action lo:*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_file_admin(deal["deal_id"], file_idx)
        )

    except Exception as e:
        log.error(f"Admin forward error: {e}")
        await _safe_send(
            ctx.bot, ADMIN_ID,
            f"⚠️ File forward mein error: `{e}`\n"
            f"Deal: `{deal['deal_id']}`\n"
            f"File: `{file_name}` ({file_type})\n\n"
            f"File DB mein save ho gayi hai.",
            parse_mode=ParseMode.MARKDOWN
        )


async def _forward_file_to_admin(bot, msg: Message, file_type: str, tg_file_id: str, caption: str):
    """File type ke hisaab se admin ko forward karo."""
    kwargs = dict(caption=caption, parse_mode=ParseMode.MARKDOWN)
    try:
        if file_type == "photo":
            return await bot.send_photo(ADMIN_ID, tg_file_id, **kwargs)
        elif file_type == "video":
            return await bot.send_video(ADMIN_ID, tg_file_id, **kwargs)
        elif file_type == "document":
            return await bot.send_document(ADMIN_ID, tg_file_id, **kwargs)
        elif file_type == "audio":
            return await bot.send_audio(ADMIN_ID, tg_file_id, **kwargs)
        elif file_type == "animation":
            return await bot.send_animation(ADMIN_ID, tg_file_id, **kwargs)
    except Exception as e:
        log.error(f"Forward to admin failed: {e}")
    return None


async def _send_file_to_user(bot, chat_id: int, file_entry: dict, caption_extra: str = ""):
    """File entry ke hisaab se user ko file bhejo."""
    file_type  = file_entry["file_type"]
    tg_file_id = file_entry["tg_file_id"]
    caption    = (file_entry.get("caption") or "") + caption_extra
    kwargs     = dict(caption=caption or None, parse_mode=ParseMode.MARKDOWN)
    try:
        if file_type == "photo":
            await bot.send_photo(chat_id, tg_file_id, **kwargs)
        elif file_type == "video":
            await bot.send_video(chat_id, tg_file_id, **kwargs)
        elif file_type == "document":
            await bot.send_document(chat_id, tg_file_id, **kwargs)
        elif file_type == "audio":
            await bot.send_audio(chat_id, tg_file_id, **kwargs)
        elif file_type == "animation":
            await bot.send_animation(chat_id, tg_file_id, **kwargs)
        return True
    except Exception as e:
        log.error(f"File send to {chat_id} failed: {e}")
        return False


# ══════════════════════════════════════════════════════════════════
#  DELIVER COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_deliver(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g or not g.get("deal_id"):
        await update.message.reply_text("❌ Koi active deal nahi!")
        return
    deal = db.get_deal(g["deal_id"])
    if not deal:
        await update.message.reply_text("❌ Deal nahi mila!")
        return
    if user.id != deal["seller_id"]:
        await update.message.reply_text("❌ Sirf seller `/deliver` kar sakta hai!")
        return
    if deal["status"] != DealStatus.PAID.value:
        await update.message.reply_text(
            f"❌ Deliver tabhi hoga jab payment confirm ho.\n"
            f"Current status: `{deal['status']}`\n\n"
            f"Pehle buyer payment kare, admin confirm kare.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    total_files = db.count_files(deal["deal_id"])
    if total_files == 0:
        await update.message.reply_text(
            "⚠️ *Koi file upload nahi ki abhi tak!*\n\n"
            "Pehle group mein photo/video/document bhejo,\n"
            "phir `/deliver` karo.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    approved = len(db.get_approved_files(deal["deal_id"]))
    pending  = total_files - approved

    await update.message.reply_text(
        f"📤 *Deliver Request Bheja!*\n\n"
        f"🆔 `{deal['deal_id']}`\n"
        f"📁 Total files: {total_files}\n"
        f"✅ Approved: {approved}\n"
        f"⏳ Pending review: {pending}\n\n"
        f"_Admin files review karke buyer ko bhejega._",
        parse_mode=ParseMode.MARKDOWN
    )

    await _safe_send(
        ctx.bot, ADMIN_ID,
        f"📦 *SELLER DELIVER READY!*\n\n"
        f"🆔 `{deal['deal_id']}`\n"
        f"📦 {deal['product']}\n"
        f"💰 ₹{deal['amount']:,.2f}\n"
        f"📁 Files: {total_files} total | {approved} approved | {pending} pending\n\n"
        f"Files buyer ko bhejne ke liye:\n"
        f"`/sendfiles {deal['deal_id']}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_admin(deal["deal_id"])
    )


# ══════════════════════════════════════════════════════════════════
#  MYFILES COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_myfiles(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g or not g.get("deal_id"):
        await update.message.reply_text("❌ Koi active deal nahi!")
        return
    deal = db.get_deal(g["deal_id"])
    if not deal or user.id != deal["seller_id"]:
        await update.message.reply_text("❌ Sirf seller `/myfiles` dekh sakta hai!")
        return

    files = db.get_files(deal["deal_id"])
    if not files:
        await update.message.reply_text(
            "📭 *Koi file upload nahi ki abhi tak.*\n\n"
            "Group mein photo/video/document bhejo!",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    lines = [f"📂 *UPLOADED FILES*\n🆔 Deal: `{deal['deal_id']}`\n"]
    for i, f in enumerate(files, 1):
        emoji  = FILE_TYPE_EMOJI.get(f["file_type"], "📁")
        status = {None: "⏳ Review Pending", True: "✅ Approved", False: "❌ Rejected"}.get(f.get("admin_approved"))
        lines.append(
            f"{i}. {emoji} `{f['file_name']}`\n"
            f"   _{f['file_type']}_ | {status}\n"
            f"   {f['uploaded_at']}"
        )

    lines.append(f"\n📊 Total: {len(files)}/{MAX_FILES_PER_DEAL}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


# ══════════════════════════════════════════════════════════════════
#  SENDFILES COMMAND — Admin files buyer ko bheje
# ══════════════════════════════════════════════════════════════════
async def cmd_sendfiles(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    if not ctx.args:
        await update.message.reply_text(
            "❌ *Usage:* `/sendfiles [deal_id]`\n\n"
            "Example: `/sendfiles ESC1A2B3C4D`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    did  = ctx.args[0].upper()
    deal = db.get_deal(did)
    if not deal:
        await update.message.reply_text(f"❌ Deal `{did}` nahi mila!", parse_mode=ParseMode.MARKDOWN)
        return

    if deal["status"] not in (DealStatus.PAID.value, DealStatus.DELIVERED.value):
        await update.message.reply_text(
            f"❌ Files tab bhejo jab payment confirm ho.\n"
            f"Current status: `{deal['status']}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    approved_files = db.get_approved_files(did)
    if not approved_files:
        total = db.count_files(did)
        await update.message.reply_text(
            f"❌ Koi approved file nahi `{did}` mein!\n"
            f"Total files: {total} (pending review)\n\n"
            f"Pehle files approve karo.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    buyer_id = deal["buyer_id"]
    sent = fail = 0

    # Buyer ko intro message
    await _safe_send(
        ctx.bot, buyer_id,
        f"📦 *PRODUCT DELIVERY!*\n\n"
        f"🆔 Deal: `{did}`\n"
        f"📦 Product: {deal['product']}\n"
        f"💰 ₹{deal['amount']:,.2f}\n\n"
        f"Tumhare files neeche hain 👇\n"
        f"_(Total: {len(approved_files)} files)_",
        parse_mode=ParseMode.MARKDOWN
    )
    await asyncio.sleep(0.5)

    for f_entry in approved_files:
        emoji        = FILE_TYPE_EMOJI.get(f_entry["file_type"], "📁")
        extra_caption = f"\n\n{emoji} _Escrow Deal: `{did}`_"
        success = await _send_file_to_user(ctx.bot, buyer_id, f_entry, extra_caption)
        if success:
            sent += 1
        else:
            fail += 1
        await asyncio.sleep(0.4)

    # Group mein announce karo
    group_id = deal["group_id"]
    await _safe_send(
        ctx.bot, group_id,
        f"📤 *FILES BUYER KO BHEJ DIYE!*\n\n"
        f"🆔 `{did}`\n"
        f"📁 Files sent: {sent}\n\n"
        f"🛒 *Buyer* — apna product check karo.\n"
        f"Sab theek ho toh `/release` karo payment release karne ke liye.",
        parse_mode=ParseMode.MARKDOWN
    )

    deal["files_sent_to_buyer"] = True
    db.set_deal(did, deal)

    if deal["status"] == DealStatus.PAID.value:
        db.transition(did, DealStatus.DELIVERED)

    await update.message.reply_text(
        f"✅ *Files Sent!*\n\n"
        f"🆔 `{did}`\n"
        f"📤 Sent: {sent} | ❌ Failed: {fail}\n\n"
        f"Buyer ko files mil gayi. Ab buyer `/release` karega.",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  CONFIRM COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/confirm [deal_id]`", parse_mode=ParseMode.MARKDOWN)
        return
    did  = ctx.args[0].upper()
    deal = db.get_deal(did)
    if not deal:
        await update.message.reply_text(f"❌ Deal `{did}` nahi mila!", parse_mode=ParseMode.MARKDOWN)
        return
    if not db.transition(did, DealStatus.PAID):
        await update.message.reply_text(
            f"❌ Transition invalid. Status: `{deal['status']}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    deal           = db.get_deal(did)
    deal["txn_id"] = _txn_id()
    db.set_deal(did, deal)

    total_files = db.count_files(did)
    file_hint = (
        f"\n\n📁 Seller ne `{total_files}` file(s) pehle se upload kar di hain!\n"
        f"Review ke baad `/sendfiles {did}` karo."
        if total_files > 0
        else "\n\n📤 Seller abhi files upload karega. Group pe nazar rakho."
    )

    await ctx.bot.send_message(
        deal["group_id"],
        f"✅ *PAYMENT CONFIRMED!*\n\n"
        f"🆔 `{did}`\n"
        f"💰 ₹{deal['amount']:,.2f} ab escrow mein secure hai 🔐\n\n"
        f"📦 *Seller* — apna product/files group mein upload karo.\n"
        f"Phir `/deliver` command use karo.",
        parse_mode=ParseMode.MARKDOWN
    )
    await update.message.reply_text(
        f"✅ Confirmed `{did}`!{file_hint}",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  RELEASE COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_release(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g or g["buyer_id"] != user.id:
        await update.message.reply_text("❌ Sirf buyer `/release` kar sakta hai!")
        return
    did  = g.get("deal_id")
    deal = db.get_deal(did) if did else None
    if not deal:
        await update.message.reply_text("❌ Koi active deal nahi!")
        return
    if not db.transition(did, DealStatus.DELIVERED):
        await update.message.reply_text(
            f"❌ Release allowed nahi abhi.\n"
            f"Status: `{deal['status']}`\n\n"
            f"Pehle files receive karo.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    await update.message.reply_text(
        f"📦 *Delivery Confirmed!*\n\n"
        f"🆔 `{did}`\n\n"
        f"*Seller* — apna UPI ID bhejo:\n"
        f"Format: `yourname@bank`",
        parse_mode=ParseMode.MARKDOWN
    )
    await _safe_send(
        ctx.bot, ADMIN_ID,
        f"🔔 *RELEASE REQUESTED*\n\n"
        f"🆔 `{did}`\n"
        f"💰 ₹{deal['amount']:,.2f}\n\n"
        f"Seller abhi UPI bhejega.\n"
        f"UPI aane ke baad `/done {did}` karo.",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  DONE COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/done [deal_id]`", parse_mode=ParseMode.MARKDOWN)
        return
    did  = ctx.args[0].upper()
    deal = db.get_deal(did)
    if not deal:
        await update.message.reply_text(f"❌ Deal `{did}` nahi mila!", parse_mode=ParseMode.MARKDOWN)
        return
    if not db.transition(did, DealStatus.COMPLETE):
        await update.message.reply_text(
            f"❌ Transition invalid. Status: `{deal['status']}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    fee, payout = _fee_amt(deal["amount"])
    await ctx.bot.send_message(
        deal["group_id"],
        f"🎊 *DEAL COMPLETE!*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 `{did}`\n"
        f"📦 {deal['product']}\n"
        f"💰 ₹{deal['amount']:,.2f}\n"
        f"💸 Seller payout: ₹{payout:,.2f}\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"🤝 Phantom Escrow pe trade karne ka shukriya!\n"
        f"_Powered by @unknown_tanveer 🔥_",
        parse_mode=ParseMode.MARKDOWN
    )
    await update.message.reply_text(
        f"✅ Deal `{did}` complete!\n"
        f"Seller ko ₹{payout:,.2f} bhejo `{deal.get('seller_upi', 'UPI not set')}`.",
        parse_mode=ParseMode.MARKDOWN
    )
    g = db.get_group(deal["group_id"])
    if g:
        g["deal_id"] = None
        g["status"] = "idle"
        db.set_group(deal["group_id"], g)


# ══════════════════════════════════════════════════════════════════
#  STATUS COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _private_history(update, ctx)
        return
    g = db.get_group(chat.id)
    if not g:
        await update.message.reply_text("❌ `/setup` se shuru karo!", parse_mode=ParseMode.MARKDOWN)
        return
    did  = g.get("deal_id")
    deal = db.get_deal(did) if did else None
    lines = [
        "📊 *DEAL STATUS*\n",
        f"🛒 Buyer: {'✅ Set' if g.get('buyer_id') else '❌ Not set'}",
        f"📦 Seller: {'✅ Set' if g.get('seller_id') else '❌ Not set'}",
    ]
    if deal:
        st          = DealStatus(deal["status"])
        total_files = db.count_files(did)
        approved    = len(db.get_approved_files(did))
        lines += [
            f"\n🆔 `{did}`",
            f"📦 {deal['product']}",
            f"💰 ₹{deal['amount']:,.2f}",
            f"📌 {STATUS_EMOJI[st]} {STATUS_LABEL[st]}",
            f"📁 Files: {total_files} uploaded | {approved} approved",
            f"⏰ Created: {deal['created_at']}",
        ]
        if deal.get("expires_at") and deal["status"] == DealStatus.PENDING.value:
            lines.append(f"⌛ Expires: {deal['expires_at']}")
    else:
        lines.append("\n📌 Koi active deal nahi.\nSeller `/deal [product] [amount]` karo.")
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_group_menu()
    )


# ══════════════════════════════════════════════════════════════════
#  HISTORY COMMAND
# ══════════════════════════════════════════════════════════════════
async def _private_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user_deals = [d for d in db._data["deals"].values()
                  if d.get("buyer_id") == uid or d.get("seller_id") == uid]
    if not user_deals:
        await update.message.reply_text("📭 Koi deal nahi abhi tak!")
        return
    lines = ["📋 *TUMHARI DEALS* (last 10)\n"]
    for d in sorted(user_deals, key=lambda x: x["created_at"], reverse=True)[:10]:
        role  = "🛒 Buyer" if d.get("buyer_id") == uid else "📦 Seller"
        st    = DealStatus(d["status"])
        files = db.count_files(d["deal_id"])
        lines.append(
            f"• `{d['deal_id']}` — {d['product']}\n"
            f"  ₹{d['amount']:,.2f} | {role} | {STATUS_EMOJI[st]} {STATUS_LABEL[st]}"
            + (f" | 📁{files}" if files else "")
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _private_history(update, ctx)


# ══════════════════════════════════════════════════════════════════
#  DISPUTE COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_dispute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g or not g.get("deal_id"):
        await update.message.reply_text("❌ Koi active deal nahi!")
        return
    reason = " ".join(ctx.args) if ctx.args else "Koi reason nahi diya"
    did    = g["deal_id"]
    deal   = db.get_deal(did)
    if deal and db.transition(did, DealStatus.DISPUTE):
        deal                  = db.get_deal(did)
        deal["dispute_reason"] = reason
        db.set_deal(did, deal)
    await update.message.reply_text(
        f"⚠️ *DISPUTE RAISE HO GAYA*\n\n"
        f"🆔 `{did}`\n"
        f"📝 Reason: {reason}\n\n"
        f"Admin ko notify kar diya gaya. Wait karo.",
        parse_mode=ParseMode.MARKDOWN
    )
    await _safe_send(
        ctx.bot, ADMIN_ID,
        f"🚨 *DISPUTE!* 🚨\n\n"
        f"Group: {chat.title}\n"
        f"Deal: `{did}`\n"
        f"By: {user.first_name} (`{user.id}`)\n"
        f"Reason: {reason}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_admin(did)
    )


# ══════════════════════════════════════════════════════════════════
#  CANCEL COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Group mein use karo!")
        return
    g = db.get_group(chat.id)
    if not g or not g.get("deal_id"):
        await update.message.reply_text("❌ Koi active deal nahi!")
        return
    did  = g["deal_id"]
    deal = db.get_deal(did)
    if not deal:
        await update.message.reply_text("❌ Deal nahi mila!")
        return
    if user.id not in (deal["seller_id"], deal["buyer_id"]) and user.id != ADMIN_ID:
        await update.message.reply_text("❌ Sirf buyer/seller/admin cancel kar sakte hain!")
        return
    if not db.transition(did, DealStatus.CANCELLED):
        await update.message.reply_text(
            f"❌ Is stage pe cancel nahi ho sakta.\n"
            f"Status: `{deal['status']}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    g["deal_id"] = None
    g["status"]  = "idle"
    db.set_group(chat.id, g)
    await update.message.reply_text(
        f"❌ *Deal Cancelled*\n\n`{did}` — {user.first_name} ne cancel kiya.",
        parse_mode=ParseMode.MARKDOWN
    )
    await _safe_send(
        ctx.bot, ADMIN_ID,
        f"❌ Deal `{did}` cancel by {user.first_name} (`{user.id}`)",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  REFUND COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_refund(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/refund [deal_id]`", parse_mode=ParseMode.MARKDOWN)
        return
    did  = ctx.args[0].upper()
    deal = db.get_deal(did)
    if not deal:
        await update.message.reply_text(f"❌ Deal `{did}` nahi mila!", parse_mode=ParseMode.MARKDOWN)
        return
    db.transition(did, DealStatus.CANCELLED)
    g = db.get_group(deal["group_id"])
    if g:
        g["deal_id"] = None
        g["status"]  = "idle"
        db.set_group(deal["group_id"], g)
    await ctx.bot.send_message(
        deal["group_id"],
        f"♻️ *REFUND INITIATED*\n\n"
        f"🆔 `{did}`\n"
        f"Admin ne refund process kiya.\n"
        f"Buyer ko ₹{deal['amount']:,.2f} wapas milenge.",
        parse_mode=ParseMode.MARKDOWN
    )
    await update.message.reply_text(
        f"✅ Refund done for `{did}`.",
        parse_mode=ParseMode.MARKDOWN
    )


# ══════════════════════════════════════════════════════════════════
#  ADMIN PANEL
# ══════════════════════════════════════════════════════════════════
async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    s           = db.stats
    active      = db.active_deals()
    total_files = sum(len(v) for v in db._data["files"].values())
    text = (
        f"👑 *ADMIN PANEL v5*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 Total: {s['total']} | ✅ Done: {s['completed']}\n"
        f"💰 Volume: ₹{s['volume']:,.2f}\n"
        f"⚠️ Disputes: {s['disputed']}\n"
        f"🔄 Active: {len(active)}\n"
        f"📁 Total Files: {total_files}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    if active:
        text += "\n*Active Deals:*\n"
        for d in active[:8]:
            st    = DealStatus(d["status"])
            files = db.count_files(d["deal_id"])
            text += f"• `{d['deal_id']}` {STATUS_EMOJI[st]} ₹{d['amount']:,.0f} — {d['product']}"
            if files:
                text += f" 📁{files}"
            text += "\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    s = db.stats
    from collections import Counter
    status_cnt = Counter(d["status"] for d in db._data["deals"].values())
    lines = [
        "📊 *STATISTICS*\n",
        f"Total Deals: {s['total']}",
        f"Completed: {s['completed']}",
        f"Volume: ₹{s['volume']:,.2f}",
        f"Disputes: {s['disputed']}\n",
        "📌 *By Status:*"
    ]
    for st_val, cnt in status_cnt.items():
        st = DealStatus(st_val)
        lines.append(f"{STATUS_EMOJI[st]} {STATUS_LABEL[st]}: {cnt}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_deals(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    active = db.active_deals()
    if not active:
        await update.message.reply_text("📭 Koi active deal nahi!")
        return
    lines = [f"📋 *ACTIVE DEALS* ({len(active)})\n"]
    for d in active:
        st   = DealStatus(d["status"])
        files = db.count_files(d["deal_id"])
        appr  = len(db.get_approved_files(d["deal_id"]))
        lines.append(
            f"🆔 `{d['deal_id']}`\n"
            f"   📦 {d['product']} | 💰 ₹{d['amount']:,.2f}\n"
            f"   {STATUS_EMOJI[st]} {STATUS_LABEL[st]}\n"
            f"   📁 Files: {files} | ✅ Approved: {appr}\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: `/broadcast [message]`", parse_mode=ParseMode.MARKDOWN)
        return
    msg  = " ".join(ctx.args)
    uids = db.all_user_ids()
    await update.message.reply_text(f"📢 {len(uids)} users ko bhej raha hoon...")
    ok = fail = 0
    for uid in uids:
        try:
            await ctx.bot.send_message(
                uid,
                f"📢 *ANNOUNCEMENT*\n\n{msg}\n\n— Phantom Escrow",
                parse_mode=ParseMode.MARKDOWN
            )
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"✅ Sent: {ok} | ❌ Failed: {fail}")


# ══════════════════════════════════════════════════════════════════
#  TEXT MESSAGE HANDLER — UPI capture
# ══════════════════════════════════════════════════════════════════
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    msg  = update.message
    text = msg.text or ""

    if chat.type not in ("group", "supergroup"):
        return

    g = db.get_group(chat.id)
    if not g or not g.get("deal_id"):
        return

    deal = db.get_deal(g["deal_id"])
    if not deal:
        return

    # Seller UPI capture — DELIVERED state mein
    if (user.id == deal["seller_id"]
            and deal["status"] == DealStatus.DELIVERED.value
            and not deal.get("seller_upi")):
        upi = _find_upi(text)
        if upi:
            deal["seller_upi"] = upi
            db.set_deal(deal["deal_id"], deal)
            await msg.reply_text(
                f"💳 *UPI Saved!*\n\n`{upi}`\n\n"
                f"Admin abhi payment bhejega.",
                parse_mode=ParseMode.MARKDOWN
            )
            await _safe_send(
                ctx.bot, ADMIN_ID,
                f"💳 *SELLER UPI RECEIVED*\n\n"
                f"🆔 `{deal['deal_id']}`\n"
                f"👤 Seller: {user.first_name}\n"
                f"💳 UPI: `{upi}`\n"
                f"💰 Payout: ₹{deal['payout']:,.2f}\n\n"
                f"Bhejo aur `/done {deal['deal_id']}` karo.",
                parse_mode=ParseMode.MARKDOWN
            )


# ══════════════════════════════════════════════════════════════════
#  CALLBACK HANDLER
# ══════════════════════════════════════════════════════════════════
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    user = q.from_user
    await q.answer()

    data   = q.data
    parts  = data.split(":", 2)
    action = parts[0]
    did    = parts[1] if len(parts) > 1 else None
    deal   = db.get_deal(did) if did else None

    # ── File approve/reject ────────────────────────────────────────
    if action in ("file_approve", "file_reject"):
        if user.id != ADMIN_ID:
            await q.answer("❌ Admin only!", show_alert=True)
            return
        if not did or len(parts) < 3:
            await q.answer("Invalid data!", show_alert=True)
            return
        file_idx = int(parts[2])
        approved = (action == "file_approve")
        if db.approve_file(did, file_idx, approved):
            status_txt = "✅ Approved" if approved else "❌ Rejected"
            files      = db.get_files(did)
            fname      = files[file_idx]["file_name"] if file_idx < len(files) else "File"
            await q.edit_message_reply_markup(None)
            await q.message.reply_text(
                f"{status_txt}: `{fname}`\n"
                f"Deal: `{did}`\n\n"
                f"Approved files bhejne ke liye: `/sendfiles {did}`",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await q.answer("File nahi mili!", show_alert=True)
        return

    # ── Menu buttons ───────────────────────────────────────────────
    if action == "menu":
        sub = parts[1] if len(parts) > 1 else ""
        if sub == "status":
            await cmd_status(update, ctx)
        elif sub == "dispute":
            await update.effective_message.reply_text(
                "⚠️ Dispute raise karne ke liye:\n"
                "`/dispute [reason]`\n\n"
                "Example: `/dispute product nahi mila`",
                parse_mode=ParseMode.MARKDOWN
            )
        elif sub == "help":
            await cmd_help(update, ctx)
        return

    # ── Deal status check ──────────────────────────────────────────
    if action == "status":
        if not deal:
            await q.answer("Deal nahi mila!", show_alert=True)
            return
        st    = DealStatus(deal["status"])
        files = db.count_files(did)
        await q.answer(
            f"{STATUS_EMOJI[st]} {STATUS_LABEL[st]} | ₹{deal['amount']:,.0f} | 📁{files} files",
            show_alert=True
        )
        return

    # ── Buyer paid button ──────────────────────────────────────────
    if action == "paid":
        if not deal:
            await q.answer("Deal nahi mila!", show_alert=True)
            return
        if user.id != deal["buyer_id"]:
            await q.answer("❌ Sirf buyer yeh button dabaaye!", show_alert=True)
            return
        if deal["status"] != DealStatus.PENDING.value:
            await q.answer(f"Status: {deal['status']} — button ab kaam nahi karega.", show_alert=True)
            return
        await q.edit_message_reply_markup(None)
        await q.message.reply_text(
            f"✅ *Payment notification bheja!*\n\n"
            f"🆔 `{did}`\n"
            f"Admin verify karega aur confirm karega.\n"
            f"Thodi der ruko.",
            parse_mode=ParseMode.MARKDOWN
        )
        await _safe_send(
            ctx.bot, ADMIN_ID,
            f"💰 *PAYMENT CLAIM*\n\n"
            f"🆔 `{did}`\n"
            f"📦 {deal['product']}\n"
            f"💰 ₹{deal['amount']:,.2f}\n"
            f"👤 Buyer: {user.first_name} (`{user.id}`)\n\n"
            f"Check karo aur confirm karo 👇",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_admin(did)
        )
        return

    # ── Cancel button ──────────────────────────────────────────────
    if action == "cancel":
        if not deal:
            await q.answer("Deal nahi mila!", show_alert=True)
            return
        if user.id not in (deal["buyer_id"], deal["seller_id"]) and user.id != ADMIN_ID:
            await q.answer("❌ Sirf buyer/seller cancel kar sakte hain!", show_alert=True)
            return
        if not db.transition(did, DealStatus.CANCELLED):
            await q.answer(f"Cancel nahi ho sakta (status: {deal['status']})", show_alert=True)
            return
        g = db.get_group(deal["group_id"])
        if g:
            g["deal_id"] = None
            g["status"]  = "idle"
            db.set_group(deal["group_id"], g)
        await q.edit_message_reply_markup(None)
        await q.message.reply_text(
            f"❌ *Deal Cancelled*\n\n`{did}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Admin buttons ──────────────────────────────────────────────
    if action == "adm_confirm":
        if user.id != ADMIN_ID:
            await q.answer("❌ Admin only!", show_alert=True)
            return
        if not deal:
            await q.answer("Deal nahi mila!", show_alert=True)
            return
        if not db.transition(did, DealStatus.PAID):
            await q.answer(f"Transition invalid! Status: {deal['status']}", show_alert=True)
            return
        deal           = db.get_deal(did)
        deal["txn_id"] = _txn_id()
        db.set_deal(did, deal)
        await ctx.bot.send_message(
            deal["group_id"],
            f"✅ *PAYMENT CONFIRMED!*\n\n"
            f"🆔 `{did}` — Escrow mein secure 🔐\n\n"
            f"📦 Seller — group mein files upload karo.\n"
            f"Phir `/deliver` karo.",
            parse_mode=ParseMode.MARKDOWN
        )
        await q.edit_message_reply_markup(None)
        await q.message.reply_text(f"✅ Confirmed `{did}`!", parse_mode=ParseMode.MARKDOWN)

    elif action == "adm_release":
        if user.id != ADMIN_ID:
            await q.answer("❌ Admin only!", show_alert=True)
            return
        await q.message.reply_text(
            f"ℹ️ Files bhejne ke liye: `/sendfiles {did}`\n"
            f"Deal complete karne ke liye: `/done {did}`",
            parse_mode=ParseMode.MARKDOWN
        )

    elif action == "adm_cancel":
        if user.id != ADMIN_ID:
            await q.answer("❌ Admin only!", show_alert=True)
            return
        if not deal:
            await q.answer("Deal nahi mila!", show_alert=True)
            return
        db.transition(did, DealStatus.CANCELLED)
        g = db.get_group(deal["group_id"])
        if g:
            g["deal_id"] = None
            g["status"]  = "idle"
            db.set_group(deal["group_id"], g)
        await ctx.bot.send_message(
            deal["group_id"],
            f"❌ Deal `{did}` admin ne cancel kiya.",
            parse_mode=ParseMode.MARKDOWN
        )
        await q.edit_message_reply_markup(None)

    elif action == "adm_details":
        if not deal:
            await q.answer("Deal nahi mila!", show_alert=True)
            return
        st          = DealStatus(deal["status"])
        total_files = db.count_files(did)
        approved    = len(db.get_approved_files(did))
        pending     = len([f for f in db.get_files(did) if f.get("admin_approved") is None])
        await q.message.reply_text(
            f"📋 *DEAL DETAILS*\n\n"
            f"🆔 `{did}`\n"
            f"📦 {deal['product']}\n"
            f"💰 ₹{deal['amount']:,.2f}\n"
            f"💸 Payout: ₹{deal['payout']:,.2f}\n"
            f"📌 {STATUS_EMOJI[st]} {STATUS_LABEL[st]}\n"
            f"🛒 Buyer: `{deal['buyer_id']}`\n"
            f"📦 Seller: `{deal['seller_id']}`\n"
            f"💳 Seller UPI: `{deal.get('seller_upi') or 'N/A'}`\n"
            f"🔢 TXN: `{deal.get('txn_id') or 'N/A'}`\n"
            f"📁 Files: {total_files} total | {approved} approved | {pending} pending\n"
            f"📤 Files sent to buyer: {'✅' if deal.get('files_sent_to_buyer') else '❌'}\n"
            f"⏰ {deal['created_at']}\n"
            f"💀 Expires: {deal['expires_at']}",
            parse_mode=ParseMode.MARKDOWN
        )

    elif action == "adm_files":
        if user.id != ADMIN_ID:
            await q.answer("❌ Admin only!", show_alert=True)
            return
        if not deal:
            await q.answer("Deal nahi mila!", show_alert=True)
            return
        files = db.get_files(did)
        if not files:
            await q.message.reply_text(f"📭 Deal `{did}` mein koi file nahi abhi tak.")
            return
        lines = [f"📂 *FILES — `{did}`*\n"]
        for i, f in enumerate(files, 1):
            emoji  = FILE_TYPE_EMOJI.get(f["file_type"], "📁")
            status = {None: "⏳ Pending", True: "✅ Approved", False: "❌ Rejected"}.get(f.get("admin_approved"))
            lines.append(f"{i}. {emoji} `{f['file_name']}` — {status}")
        lines.append(f"\n`/sendfiles {did}` se approved files bhejo buyer ko.")
        await q.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    elif action == "adm_sendfiles":
        if user.id != ADMIN_ID:
            await q.answer("❌ Admin only!", show_alert=True)
            return
        # ctx.args set karke cmd_sendfiles call karo
        class FakeUpdate:
            effective_user = q.from_user
            message = q.message
        fake_ctx       = ctx
        fake_ctx.args  = [did]
        await cmd_sendfiles(FakeUpdate(), fake_ctx)


# ══════════════════════════════════════════════════════════════════
#  COMMANDS REGISTRATION
# ══════════════════════════════════════════════════════════════════
async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start",     "Welcome & bot info"),
        BotCommand("help",      "Saare commands dekho"),
        BotCommand("setup",     "Group mein bot setup karo"),
        BotCommand("buyer",     "Buyer ke roop mein register karo"),
        BotCommand("seller",    "Seller ke roop mein register karo"),
        BotCommand("deal",      "Naya deal banao (seller)"),
        BotCommand("deliver",   "Files deliver karo (seller)"),
        BotCommand("myfiles",   "Uploaded files dekho (seller)"),
        BotCommand("release",   "Product mila, payment release karo (buyer)"),
        BotCommand("status",    "Deal status dekho"),
        BotCommand("dispute",   "Dispute raise karo"),
        BotCommand("cancel",    "Deal cancel karo"),
        BotCommand("history",   "Apni deal history"),
    ])
    log.info("Bot commands registered ✅")


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║    🔐 PHANTOM ESCROW BOT v5.0 — UNKNOWNBABU          ║")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"║  Admin ID  : {ADMIN_ID}")
    print(f"║  UPI       : {ADMIN_UPI}")
    print(f"║  DB File   : {DATA_FILE}")
    print(f"║  QR File   : {'✅ Found — local use karega' if os.path.exists(QR_FILE) else '⚠️  Not found — API QR use hogi'}")
    print(f"║  Fee       : {SERVICE_FEE_PCT}%  |  Expiry: {DEAL_EXPIRE_HR}h")
    print(f"║  Max Files : {MAX_FILES_PER_DEAL} per deal")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # ── Command handlers ──────────────────────────────────────────
    commands = [
        ("start",     cmd_start),
        ("help",      cmd_help),
        ("setup",     cmd_setup),
        ("buyer",     cmd_buyer),
        ("seller",    cmd_seller),
        ("deal",      cmd_deal),
        ("deliver",   cmd_deliver),
        ("myfiles",   cmd_myfiles),
        ("release",   cmd_release),
        ("status",    cmd_status),
        ("dispute",   cmd_dispute),
        ("cancel",    cmd_cancel),
        ("history",   cmd_history),
        # Admin
        ("admin",     cmd_admin),
        ("confirm",   cmd_confirm),
        ("sendfiles", cmd_sendfiles),
        ("done",      cmd_done),
        ("refund",    cmd_refund),
        ("stats",     cmd_stats),
        ("deals",     cmd_deals),
        ("broadcast", cmd_broadcast),
    ]
    for name, fn in commands:
        app.add_handler(CommandHandler(name, fn))

    # ── Media handler ──────────────────────────────────────────────
    media_filter = (
        filters.PHOTO | filters.VIDEO | filters.Document.ALL |
        filters.AUDIO | filters.ANIMATION
    )
    app.add_handler(MessageHandler(media_filter, handle_media))

    # ── Text handler — UPI capture ─────────────────────────────────
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # ── Callback handler ───────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(handle_callback))

    print("✅ Bot chal raha hai! Ctrl+C se band karo.\n")
             loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
