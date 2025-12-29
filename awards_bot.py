# ==============================
# AWARDS BOT (STANDALONE) — PART 1
# Includes: Flask keep-alive, GitHub JSON persistence (SHA-safe), schema,
# permissions/allowed roles, channels, suggestions (pre-run), questions CRUD,
# open/lock submissions, persistent "Suggest" + "Start/Continue" buttons,
# fill wizard + /awards fill backup entry flow.
#
# PART 2 will add: reveal (mode 1/2), reveal controls, anonymous stats per result,
# chaos button + stats, auto-archive on reveal, /awards history, startup + command wiring.
# ==============================

from __future__ import annotations

import os
import json
import base64
import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import discord
from discord import app_commands
from flask import Flask

# =========================================================
# Flask keep-alive (Render)
# =========================================================
app = Flask("awards-bot")

@app.get("/")
def home():
    return "🏆 Awards bot is alive"

def _run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

threading.Thread(target=_run_flask, daemon=True).start()

# =========================================================
# ENV
# =========================================================
TOKEN = os.getenv("TOKEN")

# GitHub JSON “API” via Contents endpoint
GITHUB_REPO = os.getenv("GITHUB_REPO")          # e.g. "saraargh/thelandingstrip-awards"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")        # PAT with repo contents access
GITHUB_PATH = os.getenv("AWARDS_DATA_PATH", "awards_data.json")

# Optional command sync speed-up
GUILD_ID = os.getenv("GUILD_ID")                # e.g. "123456789012345678"

DEFAULT_SUBMISSION_DAYS = int(os.getenv("DEFAULT_SUBMISSION_DAYS", "7"))
DEFAULT_TOP_N = int(os.getenv("DEFAULT_TOP_N", "3"))

# =========================================================
# Intents
# =========================================================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = False

# =========================================================
# Time helpers
# =========================================================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)

def human_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def trim(s: str, n: int = 120) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"

# =========================================================
# GitHub JSON Store (SHA-safe, retries on 409 conflicts)
# =========================================================
class RemoteStoreError(RuntimeError):
    pass

class GitHubJSONStore:
    def __init__(self, repo: str, token: str, path: str):
        if not repo or not token or not path:
            raise RemoteStoreError("Missing GITHUB_REPO / GITHUB_TOKEN / AWARDS_DATA_PATH")
        self.repo = repo
        self.token = token
        self.path = path
        self.url = f"https://api.github.com/repos/{repo}/contents/{path}"

    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "awards-bot",
        }

    async def load(self, session: aiohttp.ClientSession) -> Tuple[Dict[str, Any], Optional[str]]:
        async with session.get(self.url, headers=self.headers()) as r:
            if r.status == 404:
                return {}, None
            if r.status >= 400:
                raise RemoteStoreError(f"GitHub GET failed ({r.status}): {await r.text()}")
            payload = await r.json()
            sha = payload.get("sha")
            content = payload.get("content") or ""
            if not content:
                return {}, sha
            raw = base64.b64decode(content.encode("utf-8"))
            return json.loads(raw.decode("utf-8")), sha

    async def save(self, session: aiohttp.ClientSession, data: Dict[str, Any], sha: Optional[str]) -> str:
        body: Dict[str, Any] = {
            "message": "awards-bot: update data",
            "content": base64.b64encode(
                json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
            ).decode("utf-8"),
        }
        if sha:
            body["sha"] = sha

        async with session.put(self.url, headers=self.headers(), json=body) as r:
            if r.status == 409:
                raise RemoteStoreError("409_CONFLICT")
            if r.status >= 400:
                raise RemoteStoreError(f"GitHub PUT failed ({r.status}): {await r.text()}")
            payload = await r.json()
            return payload.get("content", {}).get("sha") or payload.get("sha") or sha or ""

# =========================================================
# Data model
# =========================================================
def default_data() -> Dict[str, Any]:
    return {
        "version": 1,
        "settings": {
            # empty => admins only
            "allowed_role_ids": []
        },
        # active run (single). Archived runs stored in "archive" (PART 2).
        "active": None,
        "archive": []
    }

def new_run(guild_id: int, name: str, created_by: int, announcement_channel_id: int) -> Dict[str, Any]:
    rid = f"{guild_id}-{int(now_utc().timestamp())}"
    return {
        "id": rid,
        "guild_id": guild_id,
        "name": name,
        "created_by": created_by,
        "created_at": iso(now_utc()),
        # setup_suggestions -> open -> locked -> reveal_in_progress (PART 2) -> archived (PART 2)
        "status": "setup_suggestions",
        "channels": {
            "announcement": announcement_channel_id,
            "suggestions": None,  # if unset -> announcement
            "results": None,      # if unset -> announcement
            "modlog": None
        },
        "public_messages": {
            "suggestions_message_id": None,
            "submissions_message_id": None,
            "chaos_message_id": None
        },
        "deadline": None,        # iso
        "suggestions": [],       # {id,text,suggested_by,at,state}
        "questions": [],         # {id,text,type,max,required,choices,order,enabled}
        "submissions": {},       # user_id(str)-> {answers:{qid:..}, submitted_at,last_updated_at}
        "reveal": {              # filled in PART 2
            "mode": None,
            "started_at": None,
            "current_index": 0,
            "computed_results": None
        }
    }

# =========================================================
# Component IDs (persistent buttons)
# =========================================================
def cid_suggest(run_id: str) -> str:
    return f"awards:suggest:{run_id}"

def cid_fill(run_id: str) -> str:
    return f"awards:fill:{run_id}"

def cid_chaos(run_id: str) -> str:
    return f"awards:chaos:{run_id}"  # used in PART 2

# =========================================================
# Public persistent views
# =========================================================
class PublicEntryView(discord.ui.View):
    """Persistent buttons that do NOT expire."""
    def __init__(self, run_id: str, include_suggest: bool, include_fill: bool):
        super().__init__(timeout=None)
        if include_suggest:
            self.add_item(discord.ui.Button(
                label="Suggest an Award",
                style=discord.ButtonStyle.secondary,
                custom_id=cid_suggest(run_id)
            ))
        if include_fill:
            self.add_item(discord.ui.Button(
                label="Start / Continue Awards",
                style=discord.ButtonStyle.primary,
                custom_id=cid_fill(run_id)
            ))

class ChaosView(discord.ui.View):
    """Exists in PART 1 so persistent view registration is stable; handler is in PART 2."""
    def __init__(self, run_id: str):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Reveal Chaos Stats",
            style=discord.ButtonStyle.danger,
            custom_id=cid_chaos(run_id)
        ))

# =========================================================
# Modals (kept minimal because Discord modal max inputs is small)
# =========================================================
class SuggestModal(discord.ui.Modal, title="Suggest an award category"):
    suggestion = discord.ui.TextInput(
        label="Your suggestion",
        placeholder="e.g. Most likely to miss their flight",
        max_length=120
    )

    def __init__(self, bot: "AwardsBotBase", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    async def on_submit(self, interaction: discord.Interaction):
        await self.bot.submit_suggestion(interaction, self.run_id, str(self.suggestion))

class AddQuestionTextModal(discord.ui.Modal, title="Add question"):
    qtext = discord.ui.TextInput(
        label="Question text",
        placeholder="e.g. Who’s this year’s biggest cunt?",
        max_length=140
    )

    def __init__(self, bot: "AwardsBotBase", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    async def on_submit(self, interaction: discord.Interaction):
        await self.bot.start_add_question_flow(interaction, self.run_id, str(self.qtext))

class AddChoiceModal(discord.ui.Modal, title="Add a choice"):
    choice = discord.ui.TextInput(label="Choice", placeholder="e.g. Option A", max_length=60)

    def __init__(self, bot: "AwardsBotBase", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    async def on_submit(self, interaction: discord.Interaction):
        await self.bot.add_multi_choice_option(interaction, self.run_id, str(self.choice))

class ShortTextAnswerModal(discord.ui.Modal):
    answer = discord.ui.TextInput(label="Your answer", placeholder="Type your answer", max_length=140)

    def __init__(self, bot: "AwardsBotBase", run_id: str, qid: str, title: str):
        super().__init__(title=trim(title, 45), timeout=300)
        self.bot = bot
        self.run_id = run_id
        self.qid = qid

    async def on_submit(self, interaction: discord.Interaction):
        await self.bot.save_answer(interaction, self.run_id, interaction.user.id, self.qid, trim(str(self.answer), 140))
        await interaction.response.send_message("✅ Saved.", ephemeral=True)

# =========================================================
# Manage Views
# =========================================================
class ManagePanelView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    @discord.ui.button(label="Allowed Roles", style=discord.ButtonStyle.secondary)
    async def allowed_roles(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_allowed_roles(interaction, self.run_id)

    @discord.ui.button(label="Channels", style=discord.ButtonStyle.secondary)
    async def channels(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_channels(interaction, self.run_id)

    @discord.ui.button(label="Review Suggestions", style=discord.ButtonStyle.secondary)
    async def review_suggestions(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_suggestion_review(interaction, self.run_id, advance=False)

    @discord.ui.button(label="Manage Questions", style=discord.ButtonStyle.secondary)
    async def manage_questions(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_questions(interaction, self.run_id)

    @discord.ui.button(label="Post Suggestion Button", style=discord.ButtonStyle.primary)
    async def post_suggest(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.post_suggestion_message(interaction, self.run_id)

    @discord.ui.button(label="Open Submissions", style=discord.ButtonStyle.success)
    async def open_submissions(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.open_submissions(interaction, self.run_id)

    @discord.ui.button(label="Lock Submissions", style=discord.ButtonStyle.secondary)
    async def lock_submissions(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.lock_submissions(interaction, self.run_id)

    @discord.ui.button(label="Reveal Results", style=discord.ButtonStyle.danger)
    async def reveal(self, interaction: discord.Interaction, _: discord.ui.Button):
        # implemented in PART 2 (we keep the button here for the panel)
        await self.bot.reveal(interaction, self.run_id)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_manage_panel(interaction, self.run_id, edit=True)

class AllowedRolesView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

        rs = discord.ui.RoleSelect(placeholder="Select role(s) to allow", min_values=1, max_values=10)
        rs.callback = self.on_select  # type: ignore
        self.rs = rs
        self.add_item(rs)

    async def on_select(self, interaction: discord.Interaction):
        await self.bot.add_allowed_roles(interaction, self.run_id, [r.id for r in self.rs.values])

    @discord.ui.button(label="Clear Allowed Roles", style=discord.ButtonStyle.danger)
    async def clear(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.set_allowed_roles(interaction, self.run_id, [])

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_manage_panel(interaction, self.run_id, edit=True)

class ChannelsView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    @discord.ui.button(label="Set Suggestions Channel", style=discord.ButtonStyle.secondary)
    async def set_suggestions(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.pick_channel(interaction, self.run_id, "suggestions")

    @discord.ui.button(label="Set Results Channel", style=discord.ButtonStyle.secondary)
    async def set_results(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.pick_channel(interaction, self.run_id, "results")

    @discord.ui.button(label="Set Mod Log Channel", style=discord.ButtonStyle.secondary)
    async def set_modlog(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.pick_channel(interaction, self.run_id, "modlog")

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_channels(interaction, self.run_id)

class ChannelPickView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str, key: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id
        self.key = key

        cs = discord.ui.ChannelSelect(
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            placeholder="Pick a channel"
        )
        cs.callback = self.on_pick  # type: ignore
        self.cs = cs
        self.add_item(cs)

    async def on_pick(self, interaction: discord.Interaction):
        ch = self.cs.values[0]
        await self.bot.set_channel(interaction, self.run_id, self.key, ch.id)

    @discord.ui.button(label="Unset (default to announcement)", style=discord.ButtonStyle.danger)
    async def unset(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.set_channel(interaction, self.run_id, self.key, None)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_channels(interaction, self.run_id)

class SuggestionReviewView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str, sug_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id
        self.sug_id = sug_id

    @discord.ui.button(label="Use as Question", style=discord.ButtonStyle.success)
    async def use(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.approve_suggestion(interaction, self.run_id, self.sug_id)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def reject(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.reject_suggestion(interaction, self.run_id, self.sug_id)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_suggestion_review(interaction, self.run_id, advance=True)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_manage_panel(interaction, self.run_id, edit=True)

class QuestionTypeView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str, qtext: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id
        self.qtext = qtext

    @discord.ui.button(label="Pick a Member", style=discord.ButtonStyle.primary)
    async def user_select(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.create_question_user_select(interaction, self.run_id, self.qtext)

    @discord.ui.button(label="Multiple Choice", style=discord.ButtonStyle.secondary)
    async def multi_choice(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.create_question_multi_choice(interaction, self.run_id, self.qtext)

    @discord.ui.button(label="Short Text", style=discord.ButtonStyle.secondary)
    async def short_text(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.create_question_short_text(interaction, self.run_id, self.qtext)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_message("Cancelled.", ephemeral=True)

class QuestionsListView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str, qids: List[str]):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

        if qids:
            opts = [discord.SelectOption(label=trim(qid, 80), value=qid) for qid in qids[:25]]
            sel = discord.ui.Select(placeholder="Select a question to manage", min_values=1, max_values=1, options=opts)
            sel.callback = self.on_pick  # type: ignore
            self.sel = sel
            self.add_item(sel)

    async def on_pick(self, interaction: discord.Interaction):
        await self.bot.show_question_actions(interaction, self.run_id, self.sel.values[0])

    @discord.ui.button(label="Add Question", style=discord.ButtonStyle.success)
    async def add(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(AddQuestionTextModal(self.bot, self.run_id))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_manage_panel(interaction, self.run_id, edit=True)

class QuestionActionsView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str, qid: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id
        self.qid = qid

    @discord.ui.button(label="Move Up", style=discord.ButtonStyle.secondary)
    async def up(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.move_question(interaction, self.run_id, self.qid, -1)

    @discord.ui.button(label="Move Down", style=discord.ButtonStyle.secondary)
    async def down(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.move_question(interaction, self.run_id, self.qid, +1)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger)
    async def remove(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.remove_question(interaction, self.run_id, self.qid)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_questions(interaction, self.run_id)

class MultiChoiceBuilderView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    @discord.ui.button(label="Add Choice", style=discord.ButtonStyle.primary)
    async def add_choice(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(AddChoiceModal(self.bot, self.run_id))

    @discord.ui.button(label="Finish", style=discord.ButtonStyle.success)
    async def finish(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.finish_multi_choice(interaction, self.run_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_message("Cancelled.", ephemeral=True)

# =========================================================
# Fill Wizard
# =========================================================
class FillWizardView(discord.ui.View):
    def __init__(self, bot: "AwardsBotBase", run_id: str, user_id: int):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id
        self.user_id = user_id

        self.btn_back = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary)
        self.btn_next = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary)
        self.btn_save = discord.ui.Button(label="Save & Exit", style=discord.ButtonStyle.secondary)
        self.btn_submit = discord.ui.Button(label="Submit All", style=discord.ButtonStyle.success)

        self.btn_back.callback = self.on_back  # type: ignore
        self.btn_next.callback = self.on_next  # type: ignore
        self.btn_save.callback = self.on_save  # type: ignore
        self.btn_submit.callback = self.on_submit  # type: ignore

        self.add_item(self.btn_back)
        self.add_item(self.btn_next)
        self.add_item(self.btn_save)
        self.add_item(self.btn_submit)

    async def on_back(self, interaction: discord.Interaction):
        await self.bot.wizard_step(interaction, self.run_id, self.user_id, -1)

    async def on_next(self, interaction: discord.Interaction):
        await self.bot.wizard_step(interaction, self.run_id, self.user_id, +1)

    async def on_save(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="💾 Saved. Come back any time before the deadline.", view=None)

    async def on_submit(self, interaction: discord.Interaction):
        await self.bot.wizard_submit(interaction, self.run_id, self.user_id)

# =========================================================
# Base bot: everything in PART 1 lives here
# PART 2 will subclass this and add: reveal/chaos/archive/history/startup/commands wiring.
# =========================================================
class AwardsBotBase(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

        self.http: Optional[aiohttp.ClientSession] = None
        self.store = GitHubJSONStore(GITHUB_REPO, GITHUB_TOKEN, GITHUB_PATH)

        self.data: Dict[str, Any] = {}
        self.sha: Optional[str] = None

        # small ephemeral cache for multi-step UI state
        self.cache: Dict[str, Any] = {}

    # ---------- setup/persistence ----------
    async def setup_hook(self):
        self.http = aiohttp.ClientSession()
        await self.reload_data()
        self.register_persistent_views()

    async def close(self):
        if self.http:
            await self.http.close()
        await super().close()

    async def reload_data(self):
        assert self.http is not None
        d, sha = await self.store.load(self.http)
        if not d:
            d = default_data()
            sha = await self._save_internal(d, sha)

        base = default_data()
        for k, v in base.items():
            d.setdefault(k, v)

        d["settings"].setdefault("allowed_role_ids", [])
        d.setdefault("archive", [])
        d.setdefault("active", None)

        self.data, self.sha = d, sha

    async def _save_internal(self, d: Dict[str, Any], sha: Optional[str]) -> str:
        assert self.http is not None
        return await self.store.save(self.http, d, sha)

    async def save_data(self):
        assert self.http is not None
        for _ in range(6):
            try:
                self.sha = await self._save_internal(self.data, self.sha)
                return
            except RemoteStoreError as e:
                if str(e) == "409_CONFLICT":
                    latest, latest_sha = await self.store.load(self.http)
                    if not latest:
                        latest = default_data()
                    # Merge: keep our latest in-memory decisions
                    latest["settings"] = self.data.get("settings", latest.get("settings", {}))
                    latest["active"] = self.data.get("active", latest.get("active"))
                    latest["archive"] = self.data.get("archive", latest.get("archive", []))
                    self.data, self.sha = latest, latest_sha
                    continue
                raise
        raise RemoteStoreError("Could not save after repeated conflicts.")

    def register_persistent_views(self):
        """Re-register persistent views on boot so buttons keep working after redeploy."""
        active = self.data.get("active")
        if isinstance(active, dict) and active.get("id"):
            rid = active["id"]
            self.add_view(PublicEntryView(rid, include_suggest=True, include_fill=True))
            self.add_view(ChaosView(rid))  # handler in PART 2

        # archived chaos buttons can exist; safe to register limited recent
        for a in (self.data.get("archive") or [])[-20:]:
            rid = a.get("id")
            if rid:
                self.add_view(ChaosView(rid))

    # ---------- access control ----------
    def has_access(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        if interaction.user.guild_permissions.administrator:
            return True
        allowed = set(self.data.get("settings", {}).get("allowed_role_ids", []))
        if not allowed:
            return False
        user_roles = {r.id for r in interaction.user.roles}
        return bool(allowed.intersection(user_roles))

    async def ensure_access(self, interaction: discord.Interaction) -> bool:
        if self.has_access(interaction):
            return True
        await interaction.response.send_message("❌ You don’t have access to manage awards.", ephemeral=True)
        return False

    # ---------- cache helpers ----------
    def _ck(self, guild_id: int, key: str) -> str:
        return f"{guild_id}:{key}"

    def cache_set(self, guild_id: int, key: str, value: Any):
        self.cache[self._ck(guild_id, key)] = {"v": value, "at": iso(now_utc())}

    def cache_get(self, guild_id: int, key: str, default: Any = None) -> Any:
        return self.cache.get(self._ck(guild_id, key), {}).get("v", default)

    # ---------- run lookup ----------
    def run_by_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        run = self.data.get("active")
        if isinstance(run, dict) and run.get("id") == run_id:
            return run
        return None

    # =========================================================
    # Persistent component handling: suggest + fill live here.
    # (chaos handled in PART 2)
    # =========================================================
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            cid = (interaction.data or {}).get("custom_id")  # type: ignore
            if isinstance(cid, str) and cid.startswith("awards:"):
                await self.handle_component(interaction, cid)
                return
        await super().on_interaction(interaction)

    async def handle_component(self, interaction: discord.Interaction, custom_id: str):
        parts = custom_id.split(":")
        if len(parts) < 3:
            return
        action = parts[1]
        run_id = ":".join(parts[2:])

        if action == "suggest":
            run = self.data.get("active")
            if not isinstance(run, dict) or run.get("id") != run_id:
                await interaction.response.send_message("This awards run is no longer active.", ephemeral=True)
                return
            if run.get("status") != "setup_suggestions":
                await interaction.response.send_message("Suggestions are closed.", ephemeral=True)
                return
            await interaction.response.send_modal(SuggestModal(self, run_id))
            return

        if action == "fill":
            run = self.data.get("active")
            if not isinstance(run, dict) or run.get("id") != run_id:
                await interaction.response.send_message("This awards run is no longer active.", ephemeral=True)
                return
            if run.get("status") != "open":
                await interaction.response.send_message("Submissions aren’t open.", ephemeral=True)
                return
            await self.start_fill(interaction, run_id)
            return

        if action == "chaos":
            # handled in PART 2
            await interaction.response.send_message("Chaos stats aren’t available yet.", ephemeral=True)
            return

    # =========================================================
    # Embeds / formatting
    # =========================================================
    def ch_fmt(self, cid: Optional[int]) -> str:
        return f"<#{cid}>" if cid else "Not set"

    def manage_embed(self, run: Dict[str, Any]) -> discord.Embed:
        ch = run["channels"]
        sug = ch.get("suggestions") or ch.get("announcement")
        res = ch.get("results") or ch.get("announcement")
        modlog = ch.get("modlog")

        pending = sum(1 for s in run.get("suggestions", []) if s.get("state") == "pending")
        allowed = self.data.get("settings", {}).get("allowed_role_ids", [])

        em = discord.Embed(title=f"🏆 {run.get('name','Awards')}", description="Awards management panel")
        em.add_field(name="Status", value=run.get("status", "unknown"), inline=False)
        em.add_field(name="Questions", value=str(len(run.get("questions", []))), inline=True)
        em.add_field(name="Suggestions (pending)", value=str(pending), inline=True)
        em.add_field(
            name="Channels",
            value=(
                f"📣 Announcement: {self.ch_fmt(ch.get('announcement'))}\n"
                f"💡 Suggestions: {self.ch_fmt(sug)}\n"
                f"🏆 Results: {self.ch_fmt(res)}\n"
                f"🛡️ Mod log: {self.ch_fmt(modlog)}"
            ),
            inline=False
        )
        em.add_field(
            name="Allowed roles",
            value=", ".join(f"<@&{rid}>" for rid in allowed) if allowed else "Admins only (none set)",
            inline=False
        )
        if run.get("deadline"):
            em.add_field(name="Deadline", value=human_utc(parse_iso(run["deadline"])), inline=False)
        return em

    # =========================================================
    # Manage panel entry (called by /awards manage in PART 2)
    # =========================================================
    async def show_manage_panel(self, interaction: discord.Interaction, run_id: str, edit: bool):
        run = self.run_by_id(run_id)
        if not run:
            if edit:
                await interaction.response.edit_message(content="No active awards run.", embed=None, view=None)
            else:
                await interaction.response.send_message("No active awards run.", ephemeral=True)
            return

        view = ManagePanelView(self, run_id)
        status = run.get("status")

        for item in view.children:
            if isinstance(item, discord.ui.Button):
                if item.label == "Open Submissions":
                    item.disabled = not (status == "setup_suggestions" and len(run.get("questions", [])) > 0)
                if item.label == "Lock Submissions":
                    item.disabled = not (status == "open")
                if item.label == "Reveal Results":
                    item.disabled = not (status == "locked")

        if edit:
            await interaction.response.edit_message(embed=self.manage_embed(run), view=view, content=None)
        else:
            await interaction.response.send_message(embed=self.manage_embed(run), view=view, ephemeral=True)

    # =========================================================
    # Allowed roles
    # =========================================================
    async def show_allowed_roles(self, interaction: discord.Interaction, run_id: str):
        allowed = self.data.get("settings", {}).get("allowed_role_ids", [])
        em = discord.Embed(title="🔑 Allowed Roles", description="Select roles that can manage awards.")
        em.add_field(
            name="Current",
            value=", ".join(f"<@&{rid}>" for rid in allowed) if allowed else "Admins only (none set)",
            inline=False
        )
        await interaction.response.edit_message(embed=em, view=AllowedRolesView(self, run_id))

    async def add_allowed_roles(self, interaction: discord.Interaction, run_id: str, role_ids: List[int]):
        allowed = set(self.data.get("settings", {}).get("allowed_role_ids", []))
        allowed.update(role_ids)
        self.data["settings"]["allowed_role_ids"] = sorted(allowed)
        await self.save_data()
        await self.show_allowed_roles(interaction, run_id)

    async def set_allowed_roles(self, interaction: discord.Interaction, run_id: str, role_ids: List[int]):
        self.data["settings"]["allowed_role_ids"] = sorted(set(role_ids))
        await self.save_data()
        await self.show_allowed_roles(interaction, run_id)

    # =========================================================
    # Channels
    # =========================================================
    async def show_channels(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return
        ch = run["channels"]
        em = discord.Embed(title="⚙️ Channels", description="Unset = defaults to announcement.")
        em.add_field(name="Suggestions", value=self.ch_fmt(ch.get("suggestions")), inline=False)
        em.add_field(name="Results", value=self.ch_fmt(ch.get("results")), inline=False)
        em.add_field(name="Mod log", value=self.ch_fmt(ch.get("modlog")), inline=False)
        await interaction.response.edit_message(embed=em, view=ChannelsView(self, run_id))

    async def pick_channel(self, interaction: discord.Interaction, run_id: str, key: str):
        await interaction.response.edit_message(
            embed=discord.Embed(title="Pick a channel", description=f"Setting: {key}"),
            view=ChannelPickView(self, run_id, key)
        )

    async def set_channel(self, interaction: discord.Interaction, run_id: str, key: str, channel_id: Optional[int]):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return

        status = run.get("status")
        if key == "suggestions" and status != "setup_suggestions":
            await interaction.response.send_message("🔒 Suggestions channel can only be changed during setup.", ephemeral=True)
            return
        if key == "results" and status == "reveal_in_progress":
            await interaction.response.send_message("🔒 Can’t change results channel during reveal.", ephemeral=True)
            return

        run["channels"][key] = channel_id
        await self.save_data()
        await self.show_channels(interaction, run_id)

    async def log_mod(self, run: Dict[str, Any], text: str):
        cid = run["channels"].get("modlog")
        if not cid:
            return
        guild = self.get_guild(run["guild_id"])
        if not guild:
            return
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.send(text)
            except Exception:
                pass

    # =========================================================
    # Suggestions (pre-run only)
    # =========================================================
    async def post_suggestion_message(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return
        if run.get("status") != "setup_suggestions":
            await interaction.response.send_message("Suggestions are not open.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("No guild.", ephemeral=True)
            return

        ch_id = run["channels"].get("suggestions") or run["channels"]["announcement"]
        ch = guild.get_channel(ch_id)
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Suggestions channel not found.", ephemeral=True)
            return

        view = PublicEntryView(run_id, include_suggest=True, include_fill=False)
        msg = await ch.send(
            f"💡 **Got an idea for _{run.get('name','the awards')}_?**\n"
            "Suggest a category you’d love to see included.\n\n"
            "👉 Click below:",
            view=view
        )
        run["public_messages"]["suggestions_message_id"] = msg.id
        await self.save_data()
        await self.log_mod(run, f"📣 Suggestions post created in {ch.mention}")
        await interaction.response.send_message(f"✅ Posted suggestion button in {ch.mention}", ephemeral=True)

    async def submit_suggestion(self, interaction: discord.Interaction, run_id: str, text: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active awards run.", ephemeral=True)
            return
        if run.get("status") != "setup_suggestions":
            await interaction.response.send_message("Suggestions are closed.", ephemeral=True)
            return

        t = trim(text, 120)
        if not t:
            await interaction.response.send_message("Please enter a suggestion.", ephemeral=True)
            return

        sid = f"sug_{int(now_utc().timestamp())}_{interaction.user.id}"
        run["suggestions"].append({
            "id": sid,
            "text": t,
            "suggested_by": interaction.user.id,
            "at": iso(now_utc()),
            "state": "pending"   # pending|approved|rejected
        })
        await self.save_data()
        await self.log_mod(run, f"💡 Suggestion submitted: **{t}** (by <@{interaction.user.id}>)")
        await interaction.response.send_message("✅ Suggestion submitted!", ephemeral=True)

    async def show_suggestion_review(self, interaction: discord.Interaction, run_id: str, advance: bool):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return

        pending = [s for s in run.get("suggestions", []) if s.get("state") == "pending"]
        if not pending:
            await interaction.response.edit_message(
                embed=discord.Embed(title="💡 Suggestions", description="No pending suggestions."),
                view=discord.ui.View()
            )
            return

        gid = interaction.guild_id or 0
        key = f"sug_idx:{interaction.user.id}"
        idx = int(self.cache_get(gid, key, 0))
        if advance:
            idx = (idx + 1) % len(pending)
        idx = clamp(idx, 0, len(pending) - 1)
        self.cache_set(gid, key, idx)

        sug = pending[idx]
        em = discord.Embed(title="💡 Review Suggestion", description=f"**{sug.get('text','')}**")
        await interaction.response.edit_message(embed=em, view=SuggestionReviewView(self, run_id, sug["id"]))

    async def approve_suggestion(self, interaction: discord.Interaction, run_id: str, sug_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return
        sug = next((s for s in run.get("suggestions", []) if s.get("id") == sug_id), None)
        if not sug or sug.get("state") != "pending":
            await interaction.response.send_message("Suggestion not available.", ephemeral=True)
            return
        sug["state"] = "approved"
        await self.save_data()
        await self.log_mod(run, f"✅ Suggestion approved: **{sug.get('text','')}**")
        await interaction.response.send_message(
            "✅ Approved. Choose question type:",
            ephemeral=True,
            view=QuestionTypeView(self, run_id, sug.get("text", ""))
        )

    async def reject_suggestion(self, interaction: discord.Interaction, run_id: str, sug_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return
        sug = next((s for s in run.get("suggestions", []) if s.get("id") == sug_id), None)
        if not sug or sug.get("state") != "pending":
            await interaction.response.send_message("Suggestion not available.", ephemeral=True)
            return
        sug["state"] = "rejected"
        await self.save_data()
        await self.log_mod(run, f"❌ Suggestion rejected: **{sug.get('text','')}**")
        await interaction.response.send_message("❌ Rejected.", ephemeral=True)

    # =========================================================
    # Questions (mods choose type; users do NOT)
    # =========================================================
    def _next_qid(self) -> str:
        return f"q_{int(now_utc().timestamp())}"

    async def show_questions(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return

        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        if not qs:
            em = discord.Embed(title="🔧 Manage Questions", description="No questions yet.")
            await interaction.response.edit_message(embed=em, view=QuestionsListView(self, run_id, []))
            return

        lines = [f"{i+1}. {q.get('text','')}" for i, q in enumerate(qs[:20])]
        em = discord.Embed(title="🔧 Manage Questions", description="\n".join(lines))
        await interaction.response.edit_message(embed=em, view=QuestionsListView(self, run_id, [q["id"] for q in qs]))

    async def show_question_actions(self, interaction: discord.Interaction, run_id: str, qid: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return
        q = next((x for x in run.get("questions", []) if x.get("id") == qid), None)
        if not q:
            await interaction.response.send_message("Not found.", ephemeral=True)
            return
        em = discord.Embed(title="Question", description=f"**{q.get('text','')}**\nType: `{q.get('type')}`")
        await interaction.response.edit_message(embed=em, view=QuestionActionsView(self, run_id, qid))

    async def start_add_question_flow(self, interaction: discord.Interaction, run_id: str, text: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return
        if run.get("status") != "setup_suggestions":
            await interaction.response.send_message("🔒 You can only add questions during setup (before submissions open).", ephemeral=True)
            return

        qtext = trim(text, 140)
        if not qtext:
            await interaction.response.send_message("Empty question.", ephemeral=True)
            return

        await interaction.response.send_message("Choose question type:", ephemeral=True, view=QuestionTypeView(self, run_id, qtext))

    async def create_question_user_select(self, interaction: discord.Interaction, run_id: str, qtext: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "setup_suggestions":
            await interaction.response.send_message("🔒 Not available right now.", ephemeral=True)
            return

        opts = [discord.SelectOption(label=str(x), value=str(x)) for x in [1, 2, 3, 5]]
        sel = discord.ui.Select(placeholder="Max selections", min_values=1, max_values=1, options=opts)

        async def _cb(i: discord.Interaction):
            max_n = int(sel.values[0])
            qid = self._next_qid()
            run["questions"].append({
                "id": qid,
                "text": trim(qtext, 140),
                "type": "user_select",
                "max": max_n,
                "required": True,
                "choices": [],
                "enabled": True,
                "order": len(run["questions"])
            })
            await self.save_data()
            await self.log_mod(run, f"➕ Question added: **{qtext}** (member pick, max {max_n})")
            await i.response.send_message(f"✅ Added member-pick question (max {max_n}).", ephemeral=True)

        sel.callback = _cb  # type: ignore
        v = discord.ui.View(timeout=300)
        v.add_item(sel)
        await interaction.response.send_message("Set max member selections:", ephemeral=True, view=v)

    async def create_question_short_text(self, interaction: discord.Interaction, run_id: str, qtext: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "setup_suggestions":
            await interaction.response.send_message("🔒 Not available right now.", ephemeral=True)
            return

        qid = self._next_qid()
        run["questions"].append({
            "id": qid,
            "text": trim(qtext, 140),
            "type": "short_text",
            "max": 1,
            "required": True,
            "choices": [],
            "enabled": True,
            "order": len(run["questions"])
        })
        await self.save_data()
        await self.log_mod(run, f"➕ Question added: **{qtext}** (short text)")
        await interaction.response.send_message("✅ Added short-text question.", ephemeral=True)

    async def create_question_multi_choice(self, interaction: discord.Interaction, run_id: str, qtext: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "setup_suggestions":
            await interaction.response.send_message("🔒 Not available right now.", ephemeral=True)
            return

        gid = interaction.guild_id or 0
        self.cache_set(gid, f"mc_qtext:{interaction.user.id}", trim(qtext, 140))
        self.cache_set(gid, f"mc_choices:{interaction.user.id}", [])
        await interaction.response.send_message(
            "Multiple choice setup:\nAdd at least **2** choices, then Finish.",
            ephemeral=True,
            view=MultiChoiceBuilderView(self, run_id)
        )

    async def add_multi_choice_option(self, interaction: discord.Interaction, run_id: str, choice: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "setup_suggestions":
            await interaction.response.send_message("Not available.", ephemeral=True)
            return

        gid = interaction.guild_id or 0
        c = trim(choice, 60)
        if not c:
            await interaction.response.send_message("Empty choice.", ephemeral=True)
            return

        choices = list(self.cache_get(gid, f"mc_choices:{interaction.user.id}", []))
        if len(choices) >= 25:
            await interaction.response.send_message("Max 25 choices.", ephemeral=True)
            return

        choices.append(c)
        self.cache_set(gid, f"mc_choices:{interaction.user.id}", choices)
        await interaction.response.send_message(f"✅ Added choice: **{c}**", ephemeral=True)

    async def finish_multi_choice(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "setup_suggestions":
            await interaction.response.send_message("Not available.", ephemeral=True)
            return

        gid = interaction.guild_id or 0
        qtext = self.cache_get(gid, f"mc_qtext:{interaction.user.id}")
        choices = list(self.cache_get(gid, f"mc_choices:{interaction.user.id}", []))

        if not qtext:
            await interaction.response.send_message("No multi-choice question in progress.", ephemeral=True)
            return
        if len(choices) < 2:
            await interaction.response.send_message("Add at least **2** choices first.", ephemeral=True)
            return

        qid = self._next_qid()
        run["questions"].append({
            "id": qid,
            "text": qtext,
            "type": "multi_choice",
            "max": 1,
            "required": True,
            "choices": choices,
            "enabled": True,
            "order": len(run["questions"])
        })
        await self.save_data()
        await self.log_mod(run, f"➕ Question added: **{qtext}** (multi choice, {len(choices)} options)")
        await interaction.response.send_message(f"✅ Added multiple choice question with {len(choices)} choices.", ephemeral=True)

    async def move_question(self, interaction: discord.Interaction, run_id: str, qid: str, delta: int):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "setup_suggestions":
            await interaction.response.send_message("🔒 You can’t reorder after submissions open.", ephemeral=True)
            return

        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        idx = next((i for i, q in enumerate(qs) if q.get("id") == qid), None)
        if idx is None:
            await interaction.response.send_message("Not found.", ephemeral=True)
            return

        new_idx = clamp(idx + delta, 0, len(qs) - 1)
        qs[idx], qs[new_idx] = qs[new_idx], qs[idx]
        for i, q in enumerate(qs):
            q["order"] = i
        run["questions"] = qs
        await self.save_data()
        await self.show_questions(interaction, run_id)

    async def remove_question(self, interaction: discord.Interaction, run_id: str, qid: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "setup_suggestions":
            await interaction.response.send_message("🔒 You can’t remove after submissions open.", ephemeral=True)
            return

        run["questions"] = [q for q in run.get("questions", []) if q.get("id") != qid]
        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        for i, q in enumerate(qs):
            q["order"] = i
        run["questions"] = qs

        await self.save_data()
        await interaction.response.send_message("🗑️ Removed question.", ephemeral=True)

    # =========================================================
    # Open / lock submissions (posts persistent fill button)
    # =========================================================
    async def open_submissions(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No active run.", ephemeral=True)
            return
        if run.get("status") != "setup_suggestions":
            await interaction.response.send_message("Submissions can’t be opened right now.", ephemeral=True)
            return
        if len(run.get("questions", [])) == 0:
            await interaction.response.send_message("Add at least 1 question first.", ephemeral=True)
            return

        if not run.get("deadline"):
            run["deadline"] = iso(now_utc() + timedelta(days=DEFAULT_SUBMISSION_DAYS))

        run["status"] = "open"
        await self.save_data()

        guild = interaction.guild
        ann = guild.get_channel(run["channels"]["announcement"]) if guild else None
        if not isinstance(ann, discord.TextChannel):
            await interaction.response.send_message("Announcement channel not found.", ephemeral=True)
            return

        view = PublicEntryView(run_id, include_suggest=False, include_fill=True)
        msg = await ann.send(
            f"🏆 **{run.get('name','Awards')}**\n"
            f"Submissions are now open.\n"
            f"⏰ Closes: **{human_utc(parse_iso(run['deadline']))}**\n\n"
            "👉 Click below to start or continue:",
            view=view
        )
        run["public_messages"]["submissions_message_id"] = msg.id
        await self.save_data()

        await self.log_mod(run, f"🟢 Submissions opened in {ann.mention}")
        await interaction.response.send_message(
            f"✅ Submissions opened in {ann.mention}.\nBackup entry: **/awards fill**",
            ephemeral=True
        )

    async def lock_submissions(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "open":
            await interaction.response.send_message("Submissions aren’t open.", ephemeral=True)
            return
        run["status"] = "locked"
        await self.save_data()
        await self.log_mod(run, "🔒 Submissions locked.")
        await interaction.response.send_message("🔒 Submissions locked.", ephemeral=True)

    # =========================================================
    # Fill wizard (users)
    # =========================================================
    async def start_fill(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run or run.get("status") != "open":
            await interaction.response.send_message("Submissions aren’t open.", ephemeral=True)
            return

        uid = interaction.user.id
        subs = run.setdefault("submissions", {})
        if str(uid) not in subs:
            subs[str(uid)] = {"answers": {}, "submitted_at": None, "last_updated_at": iso(now_utc())}
            await self.save_data()

        gid = interaction.guild_id or 0
        self.cache_set(gid, f"wiz_idx:{uid}", 0)
        await self.render_wizard(interaction, run_id, uid, first_send=True)

    async def wizard_step(self, interaction: discord.Interaction, run_id: str, user_id: int, delta: int):
        gid = interaction.guild_id or 0
        idx = int(self.cache_get(gid, f"wiz_idx:{user_id}", 0))

        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No run.", ephemeral=True)
            return

        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        idx = clamp(idx + delta, 0, max(0, len(qs) - 1))
        self.cache_set(gid, f"wiz_idx:{user_id}", idx)

        await self.render_wizard(interaction, run_id, user_id, first_send=False)

    async def render_wizard(self, interaction: discord.Interaction, run_id: str, user_id: int, first_send: bool):
        run = self.run_by_id(run_id)
        if not run:
            if first_send:
                await interaction.response.send_message("No active run.", ephemeral=True)
            else:
                await interaction.response.edit_message(content="No active run.", view=None)
            return

        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        if not qs:
            if first_send:
                await interaction.response.send_message("No questions configured.", ephemeral=True)
            else:
                await interaction.response.edit_message(content="No questions configured.", view=None)
            return

        gid = interaction.guild_id or 0
        idx = int(self.cache_get(gid, f"wiz_idx:{user_id}", 0))
        idx = clamp(idx, 0, len(qs) - 1)
        q = qs[idx]

        sub = run.get("submissions", {}).get(str(user_id), {"answers": {}})
        answers = sub.get("answers", {})
        current = answers.get(q["id"])

        header = (
            f"🏆 **{run.get('name','Awards')}**\n"
            f"Question **{idx+1}/{len(qs)}**\n\n"
            f"**{q.get('text','')}**"
        )
        if current is not None:
            header += f"\n\n✅ Saved: {self.format_answer_preview(q, current)}"

        view = FillWizardView(self, run_id, user_id)
        view.btn_back.disabled = (idx == 0)
        view.btn_next.disabled = (idx == len(qs) - 1)

        # question input controls
        if q.get("type") == "user_select":
            us = discord.ui.UserSelect(
                placeholder="Pick member(s)",
                min_values=1,
                max_values=int(q.get("max", 1))
            )

            async def _cb(i: discord.Interaction):
                vals = [u.id for u in us.values]
                v = vals if int(q.get("max", 1)) > 1 else vals[0]
                await self.save_answer(i, run_id, user_id, q["id"], v)
                await i.response.defer(ephemeral=True)

            us.callback = _cb  # type: ignore
            view.add_item(us)

        elif q.get("type") == "multi_choice":
            opts = [discord.SelectOption(label=trim(c, 80), value=c) for c in (q.get("choices") or [])[:25]]
            sel = discord.ui.Select(placeholder="Choose", min_values=1, max_values=1, options=opts)

            async def _cb(i: discord.Interaction):
                await self.save_answer(i, run_id, user_id, q["id"], sel.values[0])
                await i.response.defer(ephemeral=True)

            sel.callback = _cb  # type: ignore
            view.add_item(sel)

        elif q.get("type") == "short_text":
            btn = discord.ui.Button(label="Answer (type)", style=discord.ButtonStyle.primary)

            async def _cb(i: discord.Interaction):
                await i.response.send_modal(ShortTextAnswerModal(self, run_id, q["id"], q.get("text", "Answer")))

            btn.callback = _cb  # type: ignore
            view.add_item(btn)

        if first_send:
            await interaction.response.send_message(header, ephemeral=True, view=view)
        else:
            await interaction.response.edit_message(content=header, view=view)

    def format_answer_preview(self, q: Dict[str, Any], ans: Any) -> str:
        if q.get("type") == "user_select":
            if isinstance(ans, list):
                return ", ".join(f"<@{x}>" for x in ans)
            return f"<@{ans}>"
        if q.get("type") == "multi_choice":
            return f"`{ans}`"
        return f"`{trim(str(ans), 80)}`"

    async def save_answer(self, interaction: discord.Interaction, run_id: str, user_id: int, qid: str, value: Any):
        run = self.run_by_id(run_id)
        if not run:
            return
        sub = run.setdefault("submissions", {}).setdefault(
            str(user_id),
            {"answers": {}, "submitted_at": None, "last_updated_at": iso(now_utc())}
        )
        sub.setdefault("answers", {})
        sub["answers"][qid] = value
        sub["last_updated_at"] = iso(now_utc())
        await self.save_data()

    async def wizard_submit(self, interaction: discord.Interaction, run_id: str, user_id: int):
        run = self.run_by_id(run_id)
        if not run:
            await interaction.response.send_message("No run.", ephemeral=True)
            return
        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        sub = run.get("submissions", {}).get(str(user_id), {"answers": {}})
        answers = sub.get("answers", {})

        missing = [q for q in qs if q.get("required", True) and q.get("id") not in answers]
        if missing:
            await interaction.response.send_message(f"❌ You’re missing **{len(missing)}** required answer(s).", ephemeral=True)
            return

        sub["submitted_at"] = iso(now_utc())
        await self.save_data()
        await interaction.response.edit_message(content="🎉 Submission complete! Thanks.", view=None)

    # =========================================================
    # Stubs (implemented in PART 2)
    # =========================================================
    async def reveal(self, interaction: discord.Interaction, run_id: str):
        await interaction.response.send_message("Reveal is not loaded yet (PART 2).", ephemeral=True)

# ===== END PART 1 =====
# ==============================
# AWARDS BOT — PART 2
# Reveal flow, results, anonymous stats, chaos button,
# auto-archive on reveal, history command, startup + commands
# ==============================

# =========================================================
# Reveal controls (one-by-one)
# =========================================================
class RevealControlsView(discord.ui.View):
    def __init__(self, bot: "AwardsBot", run_id: str):
        super().__init__(timeout=900)
        self.bot = bot
        self.run_id = run_id

    @discord.ui.button(label="Reveal Next", style=discord.ButtonStyle.primary)
    async def next(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.reveal_next(interaction, self.run_id)

    @discord.ui.button(label="End & Dump Remaining", style=discord.ButtonStyle.danger)
    async def dump(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.end_reveal_dump(interaction, self.run_id)

# =========================================================
# Concrete bot (extends base)
# =========================================================
class AwardsBot(AwardsBotBase):
    # ---------------- Reveal chooser ----------------
    async def reveal(self, interaction: discord.Interaction, run_id: str):
        if not await self.ensure_access(interaction):
            return

        run = self.run_by_id(run_id)
        if not run or run.get("status") != "locked":
            await interaction.response.send_message("🔒 Lock submissions first.", ephemeral=True)
            return

        v = discord.ui.View(timeout=300)

        async def _all(i: discord.Interaction):
            await self._do_reveal(i, run_id, "all")

        async def _step(i: discord.Interaction):
            await self._do_reveal(i, run_id, "step")

        v.add_item(discord.ui.Button(label="Release All at Once", style=discord.ButtonStyle.primary, callback=_all))
        v.add_item(discord.ui.Button(label="Reveal One by One", style=discord.ButtonStyle.secondary, callback=_step))
        v.add_item(discord.ui.Button(label="Cancel", style=discord.ButtonStyle.danger))

        await interaction.response.send_message(
            f"🏆 **{run['name']}**\nHow would you like to reveal the results?",
            ephemeral=True,
            view=v
        )

    async def _do_reveal(self, interaction: discord.Interaction, run_id: str, mode: str):
        run = self.run_by_id(run_id)
        guild = interaction.guild
        ch_id = run["channels"].get("results") or run["channels"]["announcement"]
        channel = guild.get_channel(ch_id)

        results = self.compute_results(run)

        run["reveal"] = {
            "mode": mode,
            "started_at": iso(now_utc()),
            "current_index": 0,
            "computed_results": results
        }
        run["status"] = "reveal_in_progress"
        await self.save_data()

        await channel.send(f"🏆 **{run['name']}**\n🎉 Let’s begin!")

        if mode == "all":
            for r in results["per_question"]:
                await channel.send(self.format_result_block(r))
            await self.finish_reveal(run, channel)
            await interaction.response.send_message("✅ Results released.", ephemeral=True)
            return

        await channel.send("🥁 One-by-one reveal starting…")
        await interaction.response.send_message(
            "Reveal controls:",
            ephemeral=True,
            view=RevealControlsView(self, run_id)
        )

    # ---------------- Reveal stepping ----------------
    async def reveal_next(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        idx = run["reveal"]["current_index"]
        results = run["reveal"]["computed_results"]["per_question"]

        guild = interaction.guild
        ch_id = run["channels"].get("results") or run["channels"]["announcement"]
        channel = guild.get_channel(ch_id)

        if idx >= len(results):
            await interaction.response.send_message("All awards revealed.", ephemeral=True)
            return

        await channel.send("🥁🥁🥁\n\n" + self.format_result_block(results[idx]))
        run["reveal"]["current_index"] += 1
        await self.save_data()

        if run["reveal"]["current_index"] >= len(results):
            await self.finish_reveal(run, channel)
            await interaction.response.send_message("🏁 Final award revealed.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"Revealed {run['reveal']['current_index']} / {len(results)}",
                ephemeral=True
            )

    async def end_reveal_dump(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        results = run["reveal"]["computed_results"]["per_question"]
        idx = run["reveal"]["current_index"]

        guild = interaction.guild
        ch_id = run["channels"].get("results") or run["channels"]["announcement"]
        channel = guild.get_channel(ch_id)

        for r in results[idx:]:
            await channel.send(self.format_result_block(r))

        await self.finish_reveal(run, channel)
        await interaction.response.send_message("Dumped remaining & archived.", ephemeral=True)

    # ---------------- Results + stats ----------------
    def compute_results(self, run: Dict[str, Any]) -> Dict[str, Any]:
        qs = sorted(run["questions"], key=lambda q: q["order"])
        subs = run["submissions"]

        per_q = []
        overall = {}

        for q in qs:
            tally = {}
            total = 0

            for sub in subs.values():
                ans = sub["answers"].get(q["id"])
                if ans is None:
                    continue

                if q["type"] == "user_select":
                    if isinstance(ans, list):
                        for u in ans:
                            tally[str(u)] = tally.get(str(u), 0) + 1
                            overall[str(u)] = overall.get(str(u), 0) + 1
                            total += 1
                    else:
                        tally[str(ans)] = tally.get(str(ans), 0) + 1
                        overall[str(ans)] = overall.get(str(ans), 0) + 1
                        total += 1
                else:
                    tally[str(ans)] = tally.get(str(ans), 0) + 1
                    total += 1

            ranked = sorted(tally.items(), key=lambda x: x[1], reverse=True)
            top = [{"key": k, "count": v, "pct": round(v / total * 100, 2)} for k, v in ranked[:3]] if total else []

            closest = None
            if len(ranked) >= 2:
                closest = ranked[0][1] - ranked[1][1]

            per_q.append({
                "qid": q["id"],
                "text": q["text"],
                "type": q["type"],
                "total_votes": total,
                "unique_nominees": len(ranked),
                "closest": closest,
                "landslide": total > 0 and ranked and ranked[0][1] / total >= 0.6,
                "top": top
            })

        chaos = {
            "closest": min((p for p in per_q if p["closest"] is not None), key=lambda x: x["closest"], default=None),
            "most_chaotic": max(per_q, key=lambda x: x["unique_nominees"], default=None),
            "most_nominated": max(overall, key=lambda k: overall[k], default=None)
        }

        return {
            "computed_at": iso(now_utc()),
            "submissions": len(subs),
            "per_question": per_q,
            "chaos": chaos
        }

    def format_result_block(self, r: Dict[str, Any]) -> str:
        lines = [f"🏆 **{r['text']}**"]
        for i, t in enumerate(r["top"]):
            medal = ["🥇", "🥈", "🥉"][i]
            name = f"<@{t['key']}>" if r["type"] == "user_select" else f"`{t['key']}`"
            lines.append(f"{medal} {name} — {t['pct']}%")

        lines += [
            "",
            "📊 **Anonymous Stats**",
            f"• Total votes: {r['total_votes']}",
            f"• Unique nominees: {r['unique_nominees']}",
            f"• Closest gap: {r['closest']}" if r["closest"] is not None else "• Closest gap: N/A",
            f"• Landslide? {'✅' if r['landslide'] else '❌'}"
        ]
        return "\n".join(lines)

    # ---------------- Chaos + archive ----------------
    async def finish_reveal(self, run: Dict[str, Any], channel: discord.TextChannel):
        await channel.send("😈 Feeling messy?", view=ChaosView(run["id"]))
        self.data["archive"].append({
            "id": run["id"],
            "guild_id": run["guild_id"],
            "name": run["name"],
            "created_at": run["created_at"],
            "ended_at": iso(now_utc()),
            "questions": run["questions"],
            "results": run["reveal"]["computed_results"]
        })
        self.data["active"] = None
        await self.save_data()

    async def handle_component(self, interaction: discord.Interaction, custom_id: str):
        if custom_id.startswith("awards:chaos:"):
            rid = custom_id.split(":", 2)[2]
            arc = next((a for a in self.data["archive"] if a["id"] == rid), None)
            if not arc:
                await interaction.response.send_message("No chaos stats.", ephemeral=True)
                return

            c = arc["results"]["chaos"]
            msg = [
                "😈 **Chaos Stats**",
                f"Closest race: **{c['closest']['text']}**" if c["closest"] else "Closest race: N/A",
                f"Most chaotic: **{c['most_chaotic']['text']}**" if c["most_chaotic"] else "Most chaotic: N/A",
                f"Most nominated: <@{c['most_nominated']}>" if c["most_nominated"] else "Most nominated: N/A"
            ]
            await interaction.response.send_message("\n".join(msg), ephemeral=False)
            return

        await super().handle_component(interaction, custom_id)

# =========================================================
# Slash commands + startup
# =========================================================
bot = AwardsBot()

@bot.tree.command(name="awards_create", description="Create a new awards run")
async def awards_create(interaction: discord.Interaction, name: str):
    if not interaction.guild or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    if bot.data.get("active"):
        await interaction.response.send_message("An awards run is already active.", ephemeral=True)
        return

    run = new_run(interaction.guild_id, name, interaction.user.id, interaction.channel_id)
    bot.data["active"] = run
    await bot.save_data()
    await interaction.response.send_message(f"🏆 **{name}** created.\nUse `/awards_manage`.", ephemeral=True)

@bot.tree.command(name="awards_manage", description="Manage the current awards")
async def awards_manage(interaction: discord.Interaction):
    run = bot.data.get("active")
    if not run:
        await interaction.response.send_message("No active awards run.", ephemeral=True)
        return
    if not await bot.ensure_access(interaction):
        return
    await bot.show_manage_panel(interaction, run["id"], edit=False)

@bot.tree.command(name="awards_fill", description="Fill in awards (backup command)")
async def awards_fill(interaction: discord.Interaction):
    run = bot.data.get("active")
    if not run or run.get("status") != "open":
        await interaction.response.send_message("Submissions aren’t open.", ephemeral=True)
        return
    await bot.start_fill(interaction, run["id"])

@bot.tree.command(name="awards_history", description="View past awards")
async def awards_history(interaction: discord.Interaction):
    if not bot.data.get("archive"):
        await interaction.response.send_message("No past awards yet.", ephemeral=True)
        return
    lines = [f"• **{a['name']}** ({a['ended_at'][:10]})" for a in bot.data["archive"]]
    await interaction.response.send_message("🏆 **Awards History**\n" + "\n".join(lines), ephemeral=True)

@bot.event
async def on_ready():
    if GUILD_ID:
        await bot.tree.sync(guild=discord.Object(id=int(GUILD_ID)))
    else:
        await bot.tree.sync()
    print(f"Logged in as {bot.user}")

if not TOKEN:
    raise RuntimeError("TOKEN env var missing")

bot.run(TOKEN)