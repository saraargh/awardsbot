from __future__ import annotations

import os
import json
import base64
import uuid
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import discord
from discord import app_commands
from discord.ext import tasks
from flask import Flask
from zoneinfo import ZoneInfo

# =========================================================
# Flask keep-alive (Render)
# =========================================================
app = Flask("awards")

@app.get("/")
def home():
    return "🏆 Awards bot is alive"

def _run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

threading.Thread(target=_run_flask, daemon=True).start()

# =========================================================
# Config
# =========================================================
TOKEN = os.getenv("TOKEN")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")          # required
GITHUB_REPO = os.getenv("GITHUB_REPO")            # e.g. "saraargh/thepilot"
AWARDS_GITHUB_FILE = os.getenv("AWARDS_GITHUB_FILE", "awards_data.json")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")

UK_TZ = ZoneInfo("Europe/London")

USER_AGENT = "awards-bot/1.0"

if not TOKEN:
    raise RuntimeError("Missing TOKEN env var")
if not GITHUB_TOKEN or not GITHUB_REPO:
    raise RuntimeError("Missing GITHUB_TOKEN or GITHUB_REPO env var")

# =========================================================
# GitHub JSON persistence (Contents API) with merge + retry
# =========================================================
GH_API = "https://api.github.com"

def _gh_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    }

def _gh_contents_url(path: str) -> str:
    return f"{GH_API}/repos/{GITHUB_REPO}/contents/{path}"

def gh_get_json(path: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Returns (data, sha). If file missing, returns ({}, None).
    """
    url = _gh_contents_url(path)
    r = requests.get(url, headers=_gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=20)
    if r.status_code == 404:
        return {}, None
    if r.status_code != 200:
        raise RuntimeError(f"GitHub GET failed ({r.status_code}): {r.text}")

    payload = r.json()
    sha = payload.get("sha")
    content_b64 = payload.get("content", "")
    raw = base64.b64decode(content_b64).decode("utf-8") if content_b64 else "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {}
    return data, sha

def _deep_merge(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge src into dst (recursive for dicts). Lists are replaced.
    """
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            dst[k] = _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst

def gh_put_json(path: str, new_data: Dict[str, Any], sha: Optional[str]) -> str:
    """
    Write JSON to GitHub. If SHA conflict occurs, refetch latest, merge, retry.
    Returns new sha.
    """
    url = _gh_contents_url(path)
    message = f"awards bot update {datetime.now(timezone.utc).isoformat()}"

    # Try up to 5 times for conflicts
    for attempt in range(5):
        body_text = json.dumps(new_data, indent=2, ensure_ascii=False)
        content_b64 = base64.b64encode(body_text.encode("utf-8")).decode("utf-8")

        payload: Dict[str, Any] = {
            "message": message,
            "content": content_b64,
            "branch": GITHUB_BRANCH,
        }
        if sha:
            payload["sha"] = sha

        r = requests.put(url, headers=_gh_headers(), json=payload, timeout=25)

        if r.status_code in (200, 201):
            out = r.json()
            return out["content"]["sha"]

        # Conflict: refetch and merge
        if r.status_code == 409:
            latest, latest_sha = gh_get_json(path)
            merged = _deep_merge(latest, new_data)
            new_data = merged
            sha = latest_sha
            time.sleep(0.25)
            continue

        raise RuntimeError(f"GitHub PUT failed ({r.status_code}): {r.text}")

    raise RuntimeError("GitHub PUT failed after retries (persistent conflicts).")

# =========================================================
# Data model helpers
# =========================================================
def now_uk_iso() -> str:
    return datetime.now(UK_TZ).isoformat(timespec="seconds")

def _normalize_answer(s: str) -> str:
    s = (s or "").strip()
    # keep simple; normalize case + collapse spaces
    s = " ".join(s.split())
    return s.lower()

def _new_awards_event(title: str, channel_id: int) -> Dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "title": title,
        "channel_id": channel_id,
        "status": "drafting",  # drafting | voting | closed | revealed
        "created_at": now_uk_iso(),
        "opened_at": None,
        "closed_at": None,
        "revealed_at": None,
        "questions": [],  # list of {id, text, created_at}
        "responses": {},  # qid -> { user_id(str) -> {"answer": str, "ts": str} }
        "reveal_meta": {
            "last_reveal_mode": None,  # "mode1" | "mode2" | "chaos"
        },
    }

def ensure_root(data: Dict[str, Any]) -> Dict[str, Any]:
    data.setdefault("settings", {})
    data["settings"].setdefault("admin", {})
    data["settings"]["admin"].setdefault("allowed_user_ids", [])   # "allowed ID"
    data["settings"]["admin"].setdefault("allowed_role_ids", [])   # optional
    data["settings"].setdefault("defaults", {})
    data["settings"]["defaults"].setdefault("public_results_history", True)
    data.setdefault("active", None)     # active event object
    data.setdefault("history", [])      # list of past events
    return data

def is_allowed_admin(interaction: discord.Interaction, data: Dict[str, Any]) -> bool:
    # guild admins always ok
    if interaction.user and interaction.user.guild_permissions.administrator:
        return True

    admin_cfg = data.get("settings", {}).get("admin", {})
    allowed_users = set(int(x) for x in admin_cfg.get("allowed_user_ids", []) if str(x).isdigit())
    if interaction.user and interaction.user.id in allowed_users:
        return True

    allowed_roles = set(int(x) for x in admin_cfg.get("allowed_role_ids", []) if str(x).isdigit())
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member and allowed_roles:
        member_roles = {r.id for r in member.roles}
        if member_roles & allowed_roles:
            return True

    return False

# =========================================================
# Discord client
# =========================================================
intents = discord.Intents.default()
intents.members = True
intents.message_content = False

class AwardsBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

        # in-memory cache (always loaded from GH on demand anyway)
        self._cache: Dict[str, Any] = {}
        self._cache_sha: Optional[str] = None
        self._cache_loaded_at: float = 0.0

    async def setup_hook(self):
        await self.tree.sync()
        print("✅ Slash commands synced")

    def load_data(self, force: bool = False) -> Tuple[Dict[str, Any], Optional[str]]:
        # small cache to reduce GitHub hits
        if not force and (time.time() - self._cache_loaded_at) < 10 and self._cache:
            return self._cache, self._cache_sha

        data, sha = gh_get_json(AWARDS_GITHUB_FILE)
        data = ensure_root(data)

        self._cache = data
        self._cache_sha = sha
        self._cache_loaded_at = time.time()
        return data, sha

    def save_data(self, data: Dict[str, Any], sha: Optional[str]) -> str:
        data = ensure_root(data)
        new_sha = gh_put_json(AWARDS_GITHUB_FILE, data, sha)

        # update cache
        self._cache = data
        self._cache_sha = new_sha
        self._cache_loaded_at = time.time()
        return new_sha

bot = AwardsBot()

# =========================================================
# UI helpers
# =========================================================
def admin_embed(data: Dict[str, Any]) -> discord.Embed:
    active = data.get("active")
    admin_cfg = data.get("settings", {}).get("admin", {})
    allowed_users = admin_cfg.get("allowed_user_ids", [])
    allowed_roles = admin_cfg.get("allowed_role_ids", [])

    em = discord.Embed(
        title="🏆 Awards Manager",
        description="Manage the current awards, questions, voting, reveal, and history.",
        colour=discord.Colour.gold(),
    )
    em.add_field(
        name="Allowed IDs (users)",
        value=", ".join(f"`{x}`" for x in allowed_users) if allowed_users else "None set",
        inline=False,
    )
    em.add_field(
        name="Allowed Roles",
        value=", ".join(f"<@&{x}>" for x in allowed_roles) if allowed_roles else "None set",
        inline=False,
    )

    if not active:
        em.add_field(name="Active Awards", value="None (create one)", inline=False)
    else:
        qn = len(active.get("questions", []))
        status = active.get("status", "drafting")
        ch = active.get("channel_id")
        em.add_field(
            name="Active Awards",
            value=(
                f"**{active.get('title','(untitled)')}**\n"
                f"Status: `{status}`\n"
                f"Channel: <#{ch}>  \n"
                f"Questions: **{qn}**"
            ),
            inline=False,
        )

    hist = data.get("history", [])
    em.set_footer(text=f"History entries: {len(hist)}")
    return em

def results_embed(event: Dict[str, Any], q: Dict[str, Any], top: List[Tuple[str, int]], total: int, unique_voters: int) -> discord.Embed:
    title = event.get("title", "Awards")
    q_text = q.get("text", "(question)")
    em = discord.Embed(
        title=f"🏆 {title}",
        description=f"**Q:** {q_text}",
        colour=discord.Colour.blurple(),
    )
    if top:
        lines = []
        for i, (ans, count) in enumerate(top[:10], start=1):
            pretty = ans if ans else "(blank)"
            lines.append(f"**{i}.** {pretty} — **{count}**")
        em.add_field(name="Results", value="\n".join(lines), inline=False)
    else:
        em.add_field(name="Results", value="No responses.", inline=False)

    # Anonymous stats drop (every question)
    em.add_field(
        name="Anonymous stats",
        value=f"Total responses: **{total}**\nUnique voters: **{unique_voters}**",
        inline=False,
    )
    return em

def compute_question_stats(event: Dict[str, Any], qid: str) -> Tuple[List[Tuple[str, int]], int, int]:
    resp = (event.get("responses") or {}).get(qid, {})  # user_id -> {answer, ts}
    total = len(resp)
    unique_voters = len(set(resp.keys()))
    counts: Dict[str, int] = {}
    # store display form (original-ish) while counting on normalized key
    display: Dict[str, str] = {}
    for uid, obj in resp.items():
        ans_raw = (obj.get("answer") or "").strip()
        key = _normalize_answer(ans_raw)
        if not key:
            key = ""
        counts[key] = counts.get(key, 0) + 1
        # keep first-seen original casing
        if key not in display:
            display[key] = ans_raw

    # sort by count desc then alpha
    top = sorted(
        [(display.get(k, k), v) for k, v in counts.items()],
        key=lambda x: (-x[1], (x[0] or "").lower()),
    )
    return top, total, unique_voters

# =========================================================
# Views (Buttons)
# =========================================================
class ManageView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # only the opener can click (keeps it tidy)
        return interaction.user and interaction.user.id == self.owner_id

    @discord.ui.button(label="Reveal Mode 1", style=discord.ButtonStyle.primary, emoji="🥇")
    async def reveal_mode1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=False)
        await do_reveal(interaction, mode="mode1")

    @discord.ui.button(label="Reveal Mode 2", style=discord.ButtonStyle.primary, emoji="🥈")
    async def reveal_mode2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=False)
        await do_reveal(interaction, mode="mode2")

    @discord.ui.button(label="Chaos", style=discord.ButtonStyle.danger, emoji="🌀")
    async def reveal_chaos(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=False)
        await do_reveal(interaction, mode="chaos")

    @discord.ui.button(label="Close Voting", style=discord.ButtonStyle.secondary, emoji="🔒")
    async def close_voting(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=False)
        await do_close(interaction)

    @discord.ui.button(label="Clear Active", style=discord.ButtonStyle.secondary, emoji="🧹")
    async def clear_active(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=False)
        await do_clear(interaction)

# =========================================================
# Core actions
# =========================================================
async def do_reveal(interaction: discord.Interaction, mode: str):
    data, sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.followup.send("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    event = data.get("active")
    if not event:
        await interaction.followup.send("No active awards to reveal.", ephemeral=True)
        return

    if event.get("status") not in ("closed", "revealed"):
        await interaction.followup.send("Close voting first (status must be `closed`).", ephemeral=True)
        return

    channel_id = int(event.get("channel_id") or 0)
    ch = interaction.guild.get_channel(channel_id) if interaction.guild else None
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        await interaction.followup.send("Configured channel is missing or not a text channel.", ephemeral=True)
        return

    questions = list(event.get("questions", []))
    if not questions:
        await interaction.followup.send("No questions to reveal.", ephemeral=True)
        return

    # Mode behavior:
    # - mode1: one message per question (nice paced)
    # - mode2: single mega summary message (compact)
    # - chaos: random order + funny header
    import random
    if mode == "chaos":
        random.shuffle(questions)

    # mark revealed
    event["status"] = "revealed"
    event["revealed_at"] = now_uk_iso()
    event.setdefault("reveal_meta", {})
    event["reveal_meta"]["last_reveal_mode"] = mode
    data["active"] = event
    bot.save_data(data, sha)

    if mode == "mode2":
        # Build a compact summary
        lines = []
        for idx, q in enumerate(questions, start=1):
            qid = q["id"]
            top, total, unique_voters = compute_question_stats(event, qid)
            top3 = top[:3]
            if top3:
                tops = " | ".join([f"{ans or '(blank)'} (**{cnt}**)" for ans, cnt in top3])
            else:
                tops = "No responses"
            lines.append(f"**{idx}.** {q['text']}\n→ {tops}\n*(responses {total}, voters {unique_voters})*")

        header = "🏆 **RESULTS DROP**"
        if mode == "chaos":
            header = "🌀🏆 **CHAOS RESULTS DROP** (no one is safe)"

        em = discord.Embed(
            title=event.get("title", "Awards Results"),
            description=f"{header}\n\n" + "\n\n".join(lines),
            colour=discord.Colour.green(),
        )
        await ch.send(embed=em)
        await interaction.followup.send("✅ Revealed in Mode 2.", ephemeral=True)
        return

    # mode1 / chaos: paced individual posts
    if mode == "chaos":
        await ch.send("🌀🏆 **CHAOS REVEAL ACTIVATED** — stats only, vibes loud, accountability low.")

    for q in questions:
        qid = q["id"]
        top, total, unique_voters = compute_question_stats(event, qid)
        em = results_embed(event, q, top, total, unique_voters)
        await ch.send(embed=em)
        await discord.utils.sleep_until(datetime.now(timezone.utc))  # tiny yield without delay

    await interaction.followup.send(f"✅ Revealed in `{mode}`.", ephemeral=True)

async def do_close(interaction: discord.Interaction):
    data, sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.followup.send("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    event = data.get("active")
    if not event:
        await interaction.followup.send("No active awards.", ephemeral=True)
        return

    if event.get("status") != "voting":
        await interaction.followup.send("Active awards aren’t in `voting` status.", ephemeral=True)
        return

    event["status"] = "closed"
    event["closed_at"] = now_uk_iso()
    data["active"] = event
    bot.save_data(data, sha)
    await interaction.followup.send("🔒 Voting closed.", ephemeral=True)

async def do_clear(interaction: discord.Interaction):
    data, sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.followup.send("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    event = data.get("active")
    if not event:
        await interaction.followup.send("No active awards to clear.", ephemeral=True)
        return

    # push into history automatically
    data.setdefault("history", [])
    data["history"].insert(0, event)
    data["active"] = None
    bot.save_data(data, sha)
    await interaction.followup.send("🧹 Cleared active awards (saved to history).", ephemeral=True)

# =========================================================
# Slash Commands
# =========================================================
awards = app_commands.Group(name="awards", description="Awards bot commands")

@awards.command(name="manage", description="Open the awards manager panel.")
async def awards_manage(interaction: discord.Interaction):
    data, _sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.response.send_message("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    em = admin_embed(data)
    view = ManageView(owner_id=interaction.user.id)
    await interaction.response.send_message(embed=em, view=view, ephemeral=True)

@awards.command(name="set_allowed_user", description="Add/remove an allowed USER ID for /awards manage access.")
@app_commands.describe(user_id="Discord user ID", mode="add or remove")
async def awards_set_allowed_user(interaction: discord.Interaction, user_id: str, mode: str):
    data, sha = bot.load_data(force=True)
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only for editing allowed IDs.", ephemeral=True)
        return

    mode = (mode or "").lower().strip()
    if mode not in ("add", "remove"):
        await interaction.response.send_message("Mode must be `add` or `remove`.", ephemeral=True)
        return

    if not user_id.isdigit():
        await interaction.response.send_message("User ID must be numeric.", ephemeral=True)
        return

    cfg = data["settings"]["admin"]
    lst = cfg.setdefault("allowed_user_ids", [])
    if mode == "add":
        if user_id not in lst:
            lst.append(user_id)
    else:
        if user_id in lst:
            lst.remove(user_id)

    bot.save_data(data, sha)
    await interaction.response.send_message(f"✅ Allowed user IDs now: {', '.join(lst) if lst else 'None'}", ephemeral=True)

@awards.command(name="set_allowed_role", description="Add/remove an allowed ROLE ID for /awards manage access.")
@app_commands.describe(role="Role to add/remove", mode="add or remove")
async def awards_set_allowed_role(interaction: discord.Interaction, role: discord.Role, mode: str):
    data, sha = bot.load_data(force=True)
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only for editing allowed roles.", ephemeral=True)
        return

    mode = (mode or "").lower().strip()
    if mode not in ("add", "remove"):
        await interaction.response.send_message("Mode must be `add` or `remove`.", ephemeral=True)
        return

    cfg = data["settings"]["admin"]
    lst = cfg.setdefault("allowed_role_ids", [])
    rid = str(role.id)

    if mode == "add":
        if rid not in lst:
            lst.append(rid)
    else:
        if rid in lst:
            lst.remove(rid)

    bot.save_data(data, sha)
    await interaction.response.send_message(
        f"✅ Allowed roles now: {', '.join(f'<@&{x}>' for x in lst) if lst else 'None'}",
        ephemeral=True,
    )

@awards.command(name="create", description="Create a new active awards event.")
@app_commands.describe(title="Awards title", channel="Channel where prompts/results are posted")
async def awards_create(interaction: discord.Interaction, title: str, channel: discord.TextChannel):
    data, sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.response.send_message("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    if data.get("active"):
        await interaction.response.send_message("There is already an active awards. Clear it first.", ephemeral=True)
        return

    data["active"] = _new_awards_event(title=title.strip() or "Awards", channel_id=channel.id)
    bot.save_data(data, sha)
    await interaction.response.send_message(f"✅ Created active awards: **{title}** in {channel.mention}", ephemeral=True)

@awards.command(name="question_add", description="Add a question (no bucket selection).")
@app_commands.describe(text="The awards question/prompt")
async def awards_question_add(interaction: discord.Interaction, text: str):
    data, sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.response.send_message("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    event = data.get("active")
    if not event:
        await interaction.response.send_message("No active awards. Create one first.", ephemeral=True)
        return
    if event.get("status") != "drafting":
        await interaction.response.send_message("You can only add questions while status is `drafting`.", ephemeral=True)
        return

    q = {"id": str(uuid.uuid4()), "text": text.strip(), "created_at": now_uk_iso()}
    event.setdefault("questions", []).append(q)
    data["active"] = event
    bot.save_data(data, sha)

    await interaction.response.send_message(f"✅ Added question ({len(event['questions'])} total).", ephemeral=True)

@awards.command(name="question_list", description="List questions on the active awards.")
async def awards_question_list(interaction: discord.Interaction):
    data, _sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.response.send_message("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    event = data.get("active")
    if not event:
        await interaction.response.send_message("No active awards.", ephemeral=True)
        return

    qs = event.get("questions", [])
    if not qs:
        await interaction.response.send_message("No questions yet.", ephemeral=True)
        return

    lines = [f"**{i}.** {q['text']}  \n`{q['id']}`" for i, q in enumerate(qs, start=1)]
    em = discord.Embed(title="Questions", description="\n\n".join(lines), colour=discord.Colour.gold())
    await interaction.response.send_message(embed=em, ephemeral=True)

@awards.command(name="question_remove", description="Remove a question by its ID.")
@app_commands.describe(question_id="Question ID (copy from question_list)")
async def awards_question_remove(interaction: discord.Interaction, question_id: str):
    data, sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.response.send_message("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    event = data.get("active")
    if not event:
        await interaction.response.send_message("No active awards.", ephemeral=True)
        return
    if event.get("status") != "drafting":
        await interaction.response.send_message("You can only remove questions while status is `drafting`.", ephemeral=True)
        return

    qs = event.get("questions", [])
    before = len(qs)
    qs = [q for q in qs if q.get("id") != question_id]
    if len(qs) == before:
        await interaction.response.send_message("Question ID not found.", ephemeral=True)
        return

    event["questions"] = qs
    # also clean responses
    event.setdefault("responses", {}).pop(question_id, None)

    data["active"] = event
    bot.save_data(data, sha)
    await interaction.response.send_message("✅ Removed question.", ephemeral=True)

@awards.command(name="open_voting", description="Open voting and post prompts to the configured channel.")
async def awards_open_voting(interaction: discord.Interaction):
    data, sha = bot.load_data(force=True)
    if not is_allowed_admin(interaction, data):
        await interaction.response.send_message("❌ You don’t have access to manage awards.", ephemeral=True)
        return

    event = data.get("active")
    if not event:
        await interaction.response.send_message("No active awards.", ephemeral=True)
        return
    if event.get("status") != "drafting":
        await interaction.response.send_message("Awards must be `drafting` to open voting.", ephemeral=True)
        return

    channel_id = int(event.get("channel_id") or 0)
    ch = interaction.guild.get_channel(channel_id) if interaction.guild else None
    if not isinstance(ch, discord.TextChannel):
        await interaction.response.send_message("Configured channel is missing or not a text channel.", ephemeral=True)
        return

    qs = event.get("questions", [])
    if not qs:
        await interaction.response.send_message("Add at least 1 question first.", ephemeral=True)
        return

    event["status"] = "voting"
    event["opened_at"] = now_uk_iso()
    data["active"] = event
    bot.save_data(data, sha)

    # Post voting instructions
    em = discord.Embed(
        title=f"🏆 {event.get('title','Awards')} — Voting Open",
        description=(
            "Use `/awards vote` to submit your answers.\n"
            "You can change your answer any time until voting closes."
        ),
        colour=discord.Colour.green(),
    )
    em.add_field(name="Questions", value="\n".join([f"**{i}.** {q['text']}" for i, q in enumerate(qs, start=1)]), inline=False)
    await ch.send(embed=em)

    await interaction.response.send_message("✅ Voting opened and prompts posted.", ephemeral=True)

@awards.command(name="vote", description="Submit or update your answers (one question at a time).")
@app_commands.describe(question_number="Which question number", answer="Your nomination/answer")
async def awards_vote(interaction: discord.Interaction, question_number: int, answer: str):
    data, sha = bot.load_data(force=True)

    event = data.get("active")
    if not event or event.get("status") != "voting":
        await interaction.response.send_message("Voting isn’t open right now.", ephemeral=True)
        return

    qs = event.get("questions", [])
    if question_number < 1 or question_number > len(qs):
        await interaction.response.send_message("Invalid question number.", ephemeral=True)
        return

    q = qs[question_number - 1]
    qid = q["id"]

    event.setdefault("responses", {})
    event["responses"].setdefault(qid, {})
    event["responses"][qid][str(interaction.user.id)] = {
        "answer": (answer or "").strip(),
        "ts": now_uk_iso(),
    }

    data["active"] = event
    bot.save_data(data, sha)

    await interaction.response.send_message(f"✅ Saved your answer for Q{question_number}.", ephemeral=True)

@awards.command(name="close_voting", description="Close voting (admin).")
async def awards_close_voting(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=False)
    await do_close(interaction)

@awards.command(name="reveal", description="Reveal results (admin).")
@app_commands.describe(mode="mode1, mode2, or chaos")
async def awards_reveal(interaction: discord.Interaction, mode: str):
    await interaction.response.defer(ephemeral=True, thinking=False)
    mode = (mode or "").lower().strip()
    if mode not in ("mode1", "mode2", "chaos"):
        await interaction.followup.send("Mode must be `mode1`, `mode2`, or `chaos`.", ephemeral=True)
        return
    await do_reveal(interaction, mode=mode)

@awards.command(name="clear", description="Clear active awards (moves to history).")
async def awards_clear(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=False)
    await do_clear(interaction)

@awards.command(name="history", description="Show past awards (latest first).")
@app_commands.describe(limit="How many to show (max 10)")
async def awards_history(interaction: discord.Interaction, limit: int = 5):
    data, _sha = bot.load_data(force=True)
    hist = data.get("history", [])
    limit = max(1, min(10, int(limit or 5)))

    if not hist:
        await interaction.response.send_message("No history yet.", ephemeral=True)
        return

    lines = []
    for i, ev in enumerate(hist[:limit], start=1):
        lines.append(
            f"**{i}.** {ev.get('title','(untitled)')} — "
            f"`{ev.get('status','?')}` — created {ev.get('created_at','?')}"
        )

    em = discord.Embed(title="🏆 Awards History", description="\n".join(lines), colour=discord.Colour.dark_gold())
    await interaction.response.send_message(embed=em, ephemeral=True)

# Add group to tree
bot.tree.add_command(awards)

# =========================================================
# Run
# =========================================================
bot.run(TOKEN)