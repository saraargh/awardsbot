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
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_PATH = os.getenv("AWARDS_DATA_PATH", "awards_data.json")
GUILD_ID = os.getenv("GUILD_ID")

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
# Helpers
# =========================================================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)

def human_dt_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M UTC")

def trim(s: str, n: int = 120) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"

def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

# =========================================================
# GitHub JSON Store (SHA-safe)
# =========================================================
class RemoteStoreError(RuntimeError):
    pass

class GitHubJSONStore:
    def __init__(self, repo: str, token: str, path: str):
        if not repo or not token or not path:
            raise RemoteStoreError("Missing GitHub config")
        self.repo = repo
        self.token = token
        self.path = path
        self.url = f"https://api.github.com/repos/{repo}/contents/{path}"

    def headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "awards-bot",
        }

    async def load(self, session: aiohttp.ClientSession):
        async with session.get(self.url, headers=self.headers()) as r:
            if r.status == 404:
                return {}, None
            if r.status >= 400:
                raise RemoteStoreError(await r.text())
            payload = await r.json()
            sha = payload.get("sha")
            raw = base64.b64decode(payload.get("content", "").encode())
            return json.loads(raw.decode()), sha

    async def save(self, session: aiohttp.ClientSession, data: dict, sha: Optional[str]):
        body = {
            "message": "awards-bot update",
            "content": base64.b64encode(
                json.dumps(data, indent=2, ensure_ascii=False).encode()
            ).decode(),
        }
        if sha:
            body["sha"] = sha

        async with session.put(self.url, headers=self.headers(), json=body) as r:
            if r.status == 409:
                raise RemoteStoreError("409")
            if r.status >= 400:
                raise RemoteStoreError(await r.text())
            out = await r.json()
            return out.get("content", {}).get("sha") or out.get("sha")

# =========================================================
# Data model / schema
# =========================================================
def default_data():
    return {
        "version": 1,
        "settings": {"allowed_role_ids": []},
        "active": None,
        "archive": []
    }

def normalise_run(run: Dict[str, Any]) -> Dict[str, Any]:
    run.setdefault("channels", {
        "announcement": None,
        "suggestions": None,
        "results": None,
        "modlog": None
    })
    run.setdefault("public_messages", {
        "suggestions_message_id": None,
        "submissions_message_id": None,
        "chaos_message_id": None
    })
    run.setdefault("suggestions", [])
    run.setdefault("questions", [])
    run.setdefault("submissions", {})
    run.setdefault("reveal", {
        "mode": None,
        "started_at": None,
        "current_index": 0,
        "computed_results": None
    })
    return run

def new_run(guild_id: int, name: str, created_by: int, ann_channel: int):
    return normalise_run({
        "id": f"{guild_id}-{int(now_utc().timestamp())}",
        "guild_id": guild_id,
        "name": name,
        "created_by": created_by,
        "created_at": iso(now_utc()),
        "status": "setup_suggestions",
        "channels": {"announcement": ann_channel},
        "deadline": None,
    })

# =========================================================
# Custom IDs
# =========================================================
def cid_suggest(rid): return f"awards:suggest:{rid}"
def cid_fill(rid): return f"awards:fill:{rid}"
def cid_chaos(rid): return f"awards:chaos:{rid}"

# =========================================================
# Public Views
# =========================================================
class PublicEntryView(discord.ui.View):
    def __init__(self, run_id: str, suggest: bool, fill: bool):
        super().__init__(timeout=None)
        if suggest:
            self.add_item(discord.ui.Button(
                label="Suggest an Award",
                style=discord.ButtonStyle.secondary,
                custom_id=cid_suggest(run_id)
            ))
        if fill:
            self.add_item(discord.ui.Button(
                label="Start / Continue Awards",
                style=discord.ButtonStyle.primary,
                custom_id=cid_fill(run_id)
            ))

class ChaosView(discord.ui.View):
    def __init__(self, run_id: str):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Reveal Chaos Stats",
            style=discord.ButtonStyle.danger,
            custom_id=cid_chaos(run_id)
        ))

# =========================================================
# Modals
# =========================================================
class SuggestModal(discord.ui.Modal, title="Suggest an award"):
    suggestion = discord.ui.TextInput(max_length=120)

    def __init__(self, bot, run_id):
        super().__init__()
        self.bot = bot
        self.run_id = run_id

    async def on_submit(self, interaction):
        await self.bot.submit_suggestion(interaction, self.run_id, str(self.suggestion))

class AddQuestionTextModal(discord.ui.Modal, title="Add Question"):
    qtext = discord.ui.TextInput(max_length=140)

    def __init__(self, bot, run_id):
        super().__init__()
        self.bot = bot
        self.run_id = run_id

    async def on_submit(self, interaction):
        await self.bot.start_add_question_flow(interaction, self.run_id, str(self.qtext))

class AddChoiceModal(discord.ui.Modal, title="Add Choice"):
    choice = discord.ui.TextInput(max_length=60)

    def __init__(self, bot, run_id):
        super().__init__()
        self.bot = bot
        self.run_id = run_id

    async def on_submit(self, interaction):
        await self.bot.add_multi_choice_option(interaction, self.run_id, str(self.choice))
        
        
# =========================================================
# More Modals (single input each)
# =========================================================
class ShortTextAnswerModal(discord.ui.Modal):
    answer = discord.ui.TextInput(label="Your answer", max_length=140)

    def __init__(self, bot: "AwardsBot", run_id: str, qid: str, title: str):
        super().__init__(title=trim(title, 45), timeout=300)
        self.bot = bot
        self.run_id = run_id
        self.qid = qid

    async def on_submit(self, interaction: discord.Interaction):
        await self.bot.save_answer(interaction, self.run_id, interaction.user.id, self.qid, trim(str(self.answer), 140))
        await self.bot.safe_respond(interaction, content="✅ Saved.", ephemeral=True)

class DeadlineDaysModal(discord.ui.Modal, title="Set submission deadline (days)"):
    days = discord.ui.TextInput(label="Days from now", placeholder="e.g. 7", max_length=3)

    def __init__(self, bot: "AwardsBot", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.days).strip()
        try:
            d = int(raw)
        except ValueError:
            await self.bot.safe_respond(interaction, content="❌ Please enter a number (e.g. 7).", ephemeral=True)
            return
        d = clamp(d, 1, 90)
        await self.bot.set_deadline_days(interaction, self.run_id, d)

# =========================================================
# Management panel views
# =========================================================
class ManagePanelView(discord.ui.View):
    def __init__(self, bot: "AwardsBot", run_id: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.run_id = run_id

    @discord.ui.button(label="Allowed Roles", style=discord.ButtonStyle.secondary)
    async def allowed_roles(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_allowed_roles(interaction, self.run_id)

    @discord.ui.button(label="Channels", style=discord.ButtonStyle.secondary)
    async def channels(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_channels(interaction, self.run_id)

    @discord.ui.button(label="Set Deadline", style=discord.ButtonStyle.secondary)
    async def deadline(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_modal(DeadlineDaysModal(self.bot, self.run_id))

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
        await self.bot.reveal(interaction, self.run_id)

    @discord.ui.button(label="End Run (no reveal)", style=discord.ButtonStyle.secondary)
    async def end_run(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.end_run_no_reveal(interaction, self.run_id)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.bot.show_manage_panel(interaction, self.run_id, edit=True)

class AllowedRolesView(discord.ui.View):
    def __init__(self, bot: "AwardsBot", run_id: str):
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
    def __init__(self, bot: "AwardsBot", run_id: str):
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
    def __init__(self, bot: "AwardsBot", run_id: str, key: str):
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
    def __init__(self, bot: "AwardsBot", run_id: str, sug_id: str):
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
    def __init__(self, bot: "AwardsBot", run_id: str, qtext: str):
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
        await self.bot.safe_respond(interaction, content="Cancelled.", ephemeral=True)

class QuestionsListView(discord.ui.View):
    def __init__(self, bot: "AwardsBot", run_id: str, qids: List[str]):
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
    def __init__(self, bot: "AwardsBot", run_id: str, qid: str):
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
    def __init__(self, bot: "AwardsBot", run_id: str):
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
        await self.bot.cancel_multi_choice(interaction, self.run_id)

# =========================================================
# Awards Bot (Core)
# =========================================================
class AwardsBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)

        self.tree = app_commands.CommandTree(self)

        self.gh_session: Optional[aiohttp.ClientSession] = None
        self.store = GitHubJSONStore(GITHUB_REPO, GITHUB_TOKEN, GITHUB_PATH)

        self.data: Dict[str, Any] = {}
        self.sha: Optional[str] = None

        # ephemeral cache: indices + tiny flow state (NOT authoritative data)
        self.cache: Dict[str, Any] = {}

        # Slash group
        self.awards = app_commands.Group(name="awards", description="Awards bot commands")
        self.tree.add_command(self.awards)

        # Commands
        self.awards.command(name="create", description="Create a new awards run (setup mode).")(self.cmd_create)
        self.awards.command(name="manage", description="Open management panel.")(self.cmd_manage)
        self.awards.command(name="open", description="Open submissions (posts Start/Continue button).")(self.cmd_open)
        self.awards.command(name="lock", description="Lock submissions.")(self.cmd_lock)
        self.awards.command(name="reveal", description="Reveal results (choose mode). Auto-archives after reveal.")(self.cmd_reveal)
        self.awards.command(name="fill", description="Fill in current awards (backup entry).")(self.cmd_fill)
        self.awards.command(name="history", description="View awards history.")(self.cmd_history)

    # -----------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------
    async def setup_hook(self):
        self.gh_session = aiohttp.ClientSession()
        await self.reload_data()
        self.register_persistent_views()

        # sync commands
        if GUILD_ID:
            g = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=g)
            await self.tree.sync(guild=g)
        else:
            await self.tree.sync()

    async def close(self):
        if self.gh_session:
            await self.gh_session.close()
        await super().close()

    # -----------------------------------------------------
    # Safe responding (prevents Unknown interaction + double responses)
    # -----------------------------------------------------
    async def safe_respond(self, interaction: discord.Interaction, content: str = "", *, ephemeral: bool = True, embed=None, view=None):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content=content, ephemeral=ephemeral, embed=embed, view=view)
            else:
                await interaction.response.send_message(content=content, ephemeral=ephemeral, embed=embed, view=view)
        except discord.NotFound:
            # Interaction expired; nothing we can do
            return

    async def safe_edit(self, interaction: discord.Interaction, *, content=None, embed=None, view=None):
        try:
            if interaction.response.is_done():
                # If it's already responded, edit the original response if possible
                # but discord.py doesn't always expose message here; fallback to followup
                await interaction.edit_original_response(content=content, embed=embed, view=view)
            else:
                await interaction.response.edit_message(content=content, embed=embed, view=view)
        except discord.NotFound:
            return

    # -----------------------------------------------------
    # Persistence: load/save GitHub JSON (409 retry merge)
    # -----------------------------------------------------
    async def reload_data(self):
        assert self.gh_session is not None
        d, sha = await self.store.load(self.gh_session)
        if not d:
            d = default_data()
            sha = await self.store.save(self.gh_session, d, None)

        base = default_data()
        for k, v in base.items():
            d.setdefault(k, v)
        d.setdefault("settings", {})
        d["settings"].setdefault("allowed_role_ids", [])
        d.setdefault("archive", [])
        d.setdefault("active", None)

        if isinstance(d.get("active"), dict):
            d["active"] = normalise_run(d["active"])

        self.data, self.sha = d, sha

    async def save_data(self):
        assert self.gh_session is not None
        for _ in range(6):
            try:
                self.sha = await self.store.save(self.gh_session, self.data, self.sha)
                return
            except RemoteStoreError as e:
                if str(e) == "409":
                    latest, latest_sha = await self.store.load(self.gh_session)
                    if not latest:
                        latest = default_data()
                    # merge top-level fields we own
                    latest["settings"] = self.data.get("settings", latest.get("settings", {}))
                    latest["active"] = self.data.get("active", latest.get("active"))
                    latest["archive"] = self.data.get("archive", latest.get("archive", []))
                    self.data, self.sha = latest, latest_sha
                    continue
                raise
        raise RemoteStoreError("Could not save after repeated conflicts.")

    # -----------------------------------------------------
    # Permissions
    # -----------------------------------------------------
    def has_access(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        if interaction.user.guild_permissions.administrator:
            return True
        allowed = set(self.data.get("settings", {}).get("allowed_role_ids", []))
        if not allowed:
            return False
        roles = {r.id for r in interaction.user.roles}
        return bool(allowed.intersection(roles))

    async def ensure_access(self, interaction: discord.Interaction) -> bool:
        if self.has_access(interaction):
            return True
        await self.safe_respond(interaction, content="❌ You don’t have access to manage awards.", ephemeral=True)
        return False

    # -----------------------------------------------------
    # Cache helpers
    # -----------------------------------------------------
    def _ck(self, interaction: discord.Interaction, key: str) -> str:
        gid = interaction.guild_id or 0
        return f"{gid}:{interaction.user.id}:{key}"

    def cache_set(self, interaction: discord.Interaction, key: str, value: Any):
        self.cache[self._ck(interaction, key)] = {"v": value, "at": iso(now_utc())}

    def cache_get(self, interaction: discord.Interaction, key: str, default: Any = None) -> Any:
        return self.cache.get(self._ck(interaction, key), {}).get("v", default)

    def cache_del(self, interaction: discord.Interaction, key: str):
        self.cache.pop(self._ck(interaction, key), None)

    # -----------------------------------------------------
    # Persistent views
    # -----------------------------------------------------
    def register_persistent_views(self):
        active = self.data.get("active")
        if isinstance(active, dict) and active.get("id"):
            rid = active["id"]
            self.add_view(PublicEntryView(rid, suggest=True, fill=True))
            self.add_view(ChaosView(rid))
        # chaos buttons for recent archived
        for a in (self.data.get("archive") or [])[-30:]:
            rid = a.get("id")
            if rid:
                self.add_view(ChaosView(rid))

    # -----------------------------------------------------
    # Run lookup
    # -----------------------------------------------------
    def run_by_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        run = self.data.get("active")
        if isinstance(run, dict) and run.get("id") == run_id:
            return run
        return None

    # -----------------------------------------------------
    # Mod log helper
    # -----------------------------------------------------
    async def log_mod(self, run: Dict[str, Any], text: str):
        cid = (run.get("channels") or {}).get("modlog")
        if not cid:
            return
        guild = self.get_guild(int(run["guild_id"]))
        if not guild:
            return
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.send(text)
            except Exception:
                pass

    # =====================================================
    # Slash Commands
    # =====================================================
    @app_commands.describe(name="Awards name", announcement_channel="Default/announcement channel")
    async def cmd_create(self, interaction: discord.Interaction, name: str, announcement_channel: discord.TextChannel):
        if not await self.ensure_access(interaction):
            return
        if not interaction.guild:
            await self.safe_respond(interaction, content="Must be used in a server.", ephemeral=True)
            return
        if isinstance(self.data.get("active"), dict):
            await self.safe_respond(interaction, content="❌ There is already an active awards run.", ephemeral=True)
            return

        self.data["active"] = new_run(
            interaction.guild.id,
            trim(name, 60),
            interaction.user.id,
            announcement_channel.id
        )
        await self.save_data()
        self.register_persistent_views()
        await self.safe_respond(interaction, content=f"✅ Created **{trim(name,60)}**. Use **/awards manage**.", ephemeral=True)

    async def cmd_manage(self, interaction: discord.Interaction):
        if not await self.ensure_access(interaction):
            return
        run = self.data.get("active")
        if not isinstance(run, dict):
            await self.safe_respond(interaction, content="No active awards run. Use **/awards create**.", ephemeral=True)
            return
        await self.show_manage_panel(interaction, run["id"], edit=False)

    async def cmd_open(self, interaction: discord.Interaction):
        if not await self.ensure_access(interaction):
            return
        run = self.data.get("active")
        if not isinstance(run, dict):
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        await self.open_submissions(interaction, run["id"])

    async def cmd_lock(self, interaction: discord.Interaction):
        if not await self.ensure_access(interaction):
            return
        run = self.data.get("active")
        if not isinstance(run, dict):
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        await self.lock_submissions(interaction, run["id"])

    async def cmd_reveal(self, interaction: discord.Interaction):
        if not await self.ensure_access(interaction):
            return
        run = self.data.get("active")
        if not isinstance(run, dict):
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        await self.reveal(interaction, run["id"])

    async def cmd_fill(self, interaction: discord.Interaction):
        run = self.data.get("active")
        if not isinstance(run, dict):
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        await self.start_fill(interaction, run["id"])

    async def cmd_history(self, interaction: discord.Interaction):
        arch = self.data.get("archive") or []
        if not arch:
            await self.safe_respond(interaction, content="No past awards yet.", ephemeral=True)
            return
        lines = []
        for a in arch[-25:][::-1]:
            ended = (a.get("ended_at") or "")[:10]
            lines.append(f"• **{a.get('name','Awards')}** — ended {ended}")
        await self.safe_respond(interaction, content="🏆 **Awards History**\n" + "\n".join(lines), ephemeral=True)

    # =====================================================
    # Interaction handler for persistent buttons
    # IMPORTANT: no super().on_interaction() call (discord.py has no such method)
    # =====================================================
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            cid = (interaction.data or {}).get("custom_id")  # type: ignore
            if isinstance(cid, str) and cid.startswith("awards:"):
                await self.handle_component(interaction, cid)
                return
        await self.process_application_commands(interaction)

    async def handle_component(self, interaction: discord.Interaction, custom_id: str):
        parts = custom_id.split(":")
        if len(parts) < 3:
            return
        action = parts[1]
        run_id = ":".join(parts[2:])

        if action == "suggest":
            run = self.data.get("active")
            if not isinstance(run, dict) or run.get("id") != run_id:
                await self.safe_respond(interaction, content="This awards run is no longer active.", ephemeral=True)
                return
            if run.get("status") != "setup_suggestions":
                await self.safe_respond(interaction, content="Suggestions are closed.", ephemeral=True)
                return
            await interaction.response.send_modal(SuggestModal(self, run_id))
            return

        if action == "fill":
            run = self.data.get("active")
            if not isinstance(run, dict) or run.get("id") != run_id:
                await self.safe_respond(interaction, content="This awards run is no longer active.", ephemeral=True)
                return
            if run.get("status") != "open":
                await self.safe_respond(interaction, content="Submissions aren’t open.", ephemeral=True)
                return
            await self.start_fill(interaction, run_id)
            return

        if action == "chaos":
            await self.send_chaos_stats(interaction, run_id)
            return

    # =====================================================
    # Manage UI helpers
    # =====================================================
    def ch_fmt(self, cid: Optional[int]) -> str:
        return f"<#{cid}>" if cid else "Not set"

    def manage_embed(self, run: Dict[str, Any]) -> discord.Embed:
        run = normalise_run(run)
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
        dl = run.get("deadline")
        if dl:
            em.add_field(name="Deadline", value=human_dt_utc(parse_iso(dl)), inline=False)

        subs = run.get("submissions", {}) or {}
        submitted = sum(1 for s in subs.values() if (s or {}).get("submitted_at"))
        em.add_field(name="Submissions", value=f"{submitted} submitted / {len(subs)} started", inline=False)
        return em

    async def show_manage_panel(self, interaction: discord.Interaction, run_id: str, edit: bool):
        run = self.run_by_id(run_id)
        if not run:
            if edit:
                await self.safe_edit(interaction, content="No active awards run.", embed=None, view=None)
            else:
                await self.safe_respond(interaction, content="No active awards run.", ephemeral=True)
            return

        run = normalise_run(run)
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
            await self.safe_edit(interaction, embed=self.manage_embed(run), view=view, content=None)
        else:
            await self.safe_respond(interaction, embed=self.manage_embed(run), view=view, ephemeral=True)

    # =====================================================
    # Allowed roles
    # =====================================================
    async def show_allowed_roles(self, interaction: discord.Interaction, run_id: str):
        allowed = self.data.get("settings", {}).get("allowed_role_ids", [])
        em = discord.Embed(title="🔑 Allowed Roles", description="Select roles that can manage awards.")
        em.add_field(
            name="Current",
            value=", ".join(f"<@&{rid}>" for rid in allowed) if allowed else "Admins only (none set)",
            inline=False
        )
        await self.safe_edit(interaction, embed=em, view=AllowedRolesView(self, run_id))

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

    # =====================================================
    # Channels
    # =====================================================
    async def show_channels(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)
        ch = run["channels"]
        em = discord.Embed(title="⚙️ Channels", description="Unset = defaults to announcement channel.")
        em.add_field(name="Suggestions", value=self.ch_fmt(ch.get("suggestions")), inline=False)
        em.add_field(name="Results", value=self.ch_fmt(ch.get("results")), inline=False)
        em.add_field(name="Mod log", value=self.ch_fmt(ch.get("modlog")), inline=False)
        await self.safe_edit(interaction, embed=em, view=ChannelsView(self, run_id))

    async def pick_channel(self, interaction: discord.Interaction, run_id: str, key: str):
        await self.safe_edit(
            interaction,
            embed=discord.Embed(title="Pick a channel", description=f"Setting: {key}"),
            view=ChannelPickView(self, run_id, key)
        )

    async def set_channel(self, interaction: discord.Interaction, run_id: str, key: str, channel_id: Optional[int]):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") == "reveal_in_progress":
            await self.safe_respond(interaction, content="🔒 You can’t change channels during reveal.", ephemeral=True)
            return

        run["channels"][key] = channel_id
        await self.save_data()
        await self.show_channels(interaction, run_id)

    # =====================================================
    # Deadline
    # =====================================================
    async def set_deadline_days(self, interaction: discord.Interaction, run_id: str, days: int):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)
        if run.get("status") not in ("setup_suggestions", "open"):
            await self.safe_respond(interaction, content="🔒 You can only set deadline during setup or while open.", ephemeral=True)
            return

        run["deadline"] = iso(now_utc() + timedelta(days=days))
        await self.save_data()
        await self.log_mod(run, f"⏰ Deadline set to {human_dt_utc(parse_iso(run['deadline']))}")
        await self.safe_respond(interaction, content=f"✅ Deadline set: **{human_dt_utc(parse_iso(run['deadline']))}**", ephemeral=True)

    # =====================================================
    # Suggestions
    # =====================================================
    async def post_suggestion_message(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
    
        run = normalise_run(run)
    
        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="Suggestions are not open.", ephemeral=True)
            return
    
        guild = interaction.guild
        ch_id = run["channels"].get("suggestions") or run["channels"]["announcement"]
        ch = guild.get_channel(ch_id) if guild else None
    
        if not isinstance(ch, discord.TextChannel):
            await self.safe_respond(interaction, content="Suggestions channel not found.", ephemeral=True)
            return
    
        pending = sum(1 for s in run.get("suggestions", []) if s.get("state") == "pending")
    
        content = (
            f"💡 **Got an idea for _{run['name']}_?**\n\n"
            f"📥 Current suggestions: **{pending}**\n"
            "Suggest an award category you’d love to see included.\n\n"
            "👉 Click below:"
        )
    
        view = PublicEntryView(run_id, suggest=True, fill=False)
    
        # ✅ ALWAYS post a new message
        msg = await ch.send(content, view=view)
    
        # Keep reference to the latest one (for mods / logs only)
        run["public_messages"]["suggestions_message_id"] = msg.id
        await self.save_data()
    
        await self.log_mod(run, f"📣 New suggestions post created in {ch.mention}")
        await self.safe_respond(
            interaction,
            content=f"✅ New suggestions post created in {ch.mention}",
            ephemeral=True
        )

    async def submit_suggestion(self, interaction: discord.Interaction, run_id: str, text: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active awards run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="Suggestions are closed.", ephemeral=True)
            return

        t = trim(text, 120)
        if not t:
            await self.safe_respond(interaction, content="Please enter a suggestion.", ephemeral=True)
            return

        sid = f"sug_{int(now_utc().timestamp())}_{interaction.user.id}"
        run["suggestions"].append({
            "id": sid,
            "text": t,
            "suggested_by": interaction.user.id,
            "at": iso(now_utc()),
            "state": "pending"
        })

        await self.save_data()
        await self.log_mod(run, f"💡 Suggestion submitted: **{t}** (by <@{interaction.user.id}>)")
        await self.safe_respond(interaction, content="✅ Suggestion submitted!", ephemeral=True)

    async def show_suggestion_review(self, interaction: discord.Interaction, run_id: str, advance: bool):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        pending = [s for s in run.get("suggestions", []) if s.get("state") == "pending"]
        if not pending:
            await self.safe_edit(
                interaction,
                embed=discord.Embed(title="💡 Suggestions", description="No pending suggestions."),
                view=discord.ui.View()
            )
            return

        idx = int(self.cache_get(interaction, "sug_idx", 0))
        if advance:
            idx = (idx + 1) % len(pending)
        idx = clamp(idx, 0, len(pending) - 1)
        self.cache_set(interaction, "sug_idx", idx)

        sug = pending[idx]
        who = sug.get("suggested_by")
        em = discord.Embed(title="💡 Review Suggestion", description=f"**{sug.get('text','')}**")
        em.set_footer(text=f"Suggested by {who} • {idx+1}/{len(pending)}")
        await self.safe_edit(interaction, embed=em, view=SuggestionReviewView(self, run_id, sug["id"]))

    async def approve_suggestion(self, interaction: discord.Interaction, run_id: str, sug_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        sug = next((s for s in run.get("suggestions", []) if s.get("id") == sug_id), None)
        if not sug or sug.get("state") != "pending":
            await self.safe_respond(interaction, content="Suggestion not available.", ephemeral=True)
            return

        sug["state"] = "approved"
        await self.save_data()
        await self.log_mod(run, f"✅ Suggestion approved: **{sug.get('text','')}**")

        await self.safe_respond(
            interaction,
            content="✅ Approved. Now choose the question type (mods decide — users don’t).",
            ephemeral=True,
            view=QuestionTypeView(self, run_id, sug.get("text", ""))
        )

    async def reject_suggestion(self, interaction: discord.Interaction, run_id: str, sug_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        sug = next((s for s in run.get("suggestions", []) if s.get("id") == sug_id), None)
        if not sug or sug.get("state") != "pending":
            await self.safe_respond(interaction, content="Suggestion not available.", ephemeral=True)
            return

        sug["state"] = "rejected"
        await self.save_data()
        await self.log_mod(run, f"❌ Suggestion rejected: **{sug.get('text','')}**")
        await self.safe_respond(interaction, content="❌ Rejected.", ephemeral=True)

    # =====================================================
    # Questions (FIXED: no overwriting list with stale data)
    # =====================================================
    def _next_qid(self) -> str:
        return f"q_{int(now_utc().timestamp())}_{int(now_utc().microsecond)}"

    async def show_questions(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        if not qs:
            em = discord.Embed(title="🔧 Manage Questions", description="No questions yet.")
            await self.safe_edit(interaction, embed=em, view=QuestionsListView(self, run_id, []))
            return

        lines = [f"{i+1}. {q.get('text','')}" for i, q in enumerate(qs[:20])]
        em = discord.Embed(title="🔧 Manage Questions", description="\n".join(lines))
        await self.safe_edit(interaction, embed=em, view=QuestionsListView(self, run_id, [q["id"] for q in qs]))

    async def show_question_actions(self, interaction: discord.Interaction, run_id: str, qid: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        q = next((x for x in run.get("questions", []) if x.get("id") == qid), None)
        if not q:
            await self.safe_respond(interaction, content="Not found.", ephemeral=True)
            return

        em = discord.Embed(
            title="Question",
            description=f"**{q.get('text','')}**\nType: `{q.get('type')}`\nOrder: `{q.get('order',0)}`"
        )
        await self.safe_edit(interaction, embed=em, view=QuestionActionsView(self, run_id, qid))

    async def start_add_question_flow(self, interaction: discord.Interaction, run_id: str, text: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="🔒 You can only add/remove questions during setup.", ephemeral=True)
            return

        qtext = trim(text, 140)
        if not qtext:
            await self.safe_respond(interaction, content="Empty question.", ephemeral=True)
            return

        await self.safe_respond(interaction, content="Choose question type:", ephemeral=True, view=QuestionTypeView(self, run_id, qtext))

    async def create_question_user_select(self, interaction: discord.Interaction, run_id: str, qtext: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="🔒 Questions are locked once submissions open.", ephemeral=True)
            return

        opts = [discord.SelectOption(label=str(x), value=str(x)) for x in [1, 2, 3, 5]]
        sel = discord.ui.Select(placeholder="Max selections", min_values=1, max_values=1, options=opts)

        async def _cb(i: discord.Interaction):
            r = self.run_by_id(run_id)
            if not r:
                await self.safe_respond(i, content="No active run.", ephemeral=True)
                return
            r = normalise_run(r)
            if r.get("status") != "setup_suggestions":
                await self.safe_respond(i, content="🔒 Questions are locked.", ephemeral=True)
                return

            max_n = int(sel.values[0])
            qid = self._next_qid()

            # mutate in place
            r.setdefault("questions", [])
            r["questions"].append({
                "id": qid,
                "text": trim(qtext, 140),
                "type": "user_select",
                "max": max_n,
                "required": True,
                "choices": [],
                "enabled": True,
                "order": len(r["questions"])
            })
            for idx, q in enumerate(sorted(r["questions"], key=lambda x: x.get("order", 0))):
                q["order"] = idx

            await self.save_data()
            await self.log_mod(r, f"➕ Question added: **{qtext}** (member pick, max {max_n})")
            await self.safe_respond(i, content=f"✅ Added member-pick question (max {max_n}).", ephemeral=True)

        sel.callback = _cb  # type: ignore
        v = discord.ui.View(timeout=300)
        v.add_item(sel)
        await self.safe_respond(interaction, content="Set max member selections:", ephemeral=True, view=v)

    async def create_question_short_text(self, interaction: discord.Interaction, run_id: str, qtext: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="🔒 Questions are locked once submissions open.", ephemeral=True)
            return

        qid = self._next_qid()
        run.setdefault("questions", [])
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
        for idx, q in enumerate(sorted(run["questions"], key=lambda x: x.get("order", 0))):
            q["order"] = idx

        await self.save_data()
        await self.log_mod(run, f"➕ Question added: **{qtext}** (short text)")
        await self.safe_respond(interaction, content="✅ Added short-text question.", ephemeral=True)

    async def create_question_multi_choice(self, interaction: discord.Interaction, run_id: str, qtext: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="🔒 Questions are locked once submissions open.", ephemeral=True)
            return

        # FIX: commit the question immediately; choices are edited onto it (no fragile cache list)
        qid = self._next_qid()
        run.setdefault("questions", [])
        run["questions"].append({
            "id": qid,
            "text": trim(qtext, 140),
            "type": "multi_choice",
            "max": 1,
            "required": True,
            "choices": [],
            "enabled": True,
            "order": len(run["questions"])
        })
        for idx, q in enumerate(sorted(run["questions"], key=lambda x: x.get("order", 0))):
            q["order"] = idx

        await self.save_data()
        self.cache_set(interaction, "mc_edit_qid", qid)

        await self.safe_respond(
            interaction,
            content="Multiple choice setup:\nAdd at least **2** choices, then Finish.",
            ephemeral=True,
            view=MultiChoiceBuilderView(self, run_id)
        )

    async def add_multi_choice_option(self, interaction: discord.Interaction, run_id: str, choice: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="Not available.", ephemeral=True)
            return

        qid = self.cache_get(interaction, "mc_edit_qid")
        if not qid:
            await self.safe_respond(interaction, content="❌ Multi-choice builder expired. Please start again.", ephemeral=True)
            return

        q = next((x for x in run.get("questions", []) if x.get("id") == qid and x.get("type") == "multi_choice"), None)
        if not q:
            await self.safe_respond(interaction, content="❌ Couldn’t find that multi-choice question. Please start again.", ephemeral=True)
            return

        c = trim(choice, 60)
        if not c:
            await self.safe_respond(interaction, content="Empty choice.", ephemeral=True)
            return

        q.setdefault("choices", [])
        if len(q["choices"]) >= 25:
            await self.safe_respond(interaction, content="Max 25 choices.", ephemeral=True)
            return

        q["choices"].append(c)
        await self.save_data()
        await self.safe_respond(interaction, content=f"✅ Added choice: **{c}**", ephemeral=True)

    async def finish_multi_choice(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        qid = self.cache_get(interaction, "mc_edit_qid")
        if not qid:
            await self.safe_respond(interaction, content="❌ Multi-choice builder expired. Please start again.", ephemeral=True)
            return

        q = next((x for x in run.get("questions", []) if x.get("id") == qid and x.get("type") == "multi_choice"), None)
        if not q:
            await self.safe_respond(interaction, content="❌ Couldn’t find that question.", ephemeral=True)
            return

        choices = q.get("choices") or []
        if len(choices) < 2:
            await self.safe_respond(interaction, content="Need at least **2** choices.", ephemeral=True)
            return

        self.cache_del(interaction, "mc_edit_qid")
        await self.save_data()
        await self.log_mod(run, f"➕ Question added: **{q.get('text','')}** (multi choice, {len(choices)} options)")
        await self.safe_respond(interaction, content=f"✅ Saved multiple choice question with {len(choices)} choices.", ephemeral=True)

    async def cancel_multi_choice(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        qid = self.cache_get(interaction, "mc_edit_qid")
        if qid:
            # remove the placeholder question
            run["questions"] = [qq for qq in run.get("questions", []) if qq.get("id") != qid]
            for idx, q in enumerate(sorted(run["questions"], key=lambda x: x.get("order", 0))):
                q["order"] = idx
            await self.save_data()

        self.cache_del(interaction, "mc_edit_qid")
        await self.safe_respond(interaction, content="Cancelled.", ephemeral=True)

    async def move_question(self, interaction: discord.Interaction, run_id: str, qid: str, delta: int):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="🔒 You can’t reorder after submissions open.", ephemeral=True)
            return

        # mutate in place: reorder by swapping order values
        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        idx = next((i for i, q in enumerate(qs) if q.get("id") == qid), None)
        if idx is None:
            await self.safe_respond(interaction, content="Not found.", ephemeral=True)
            return

        new_idx = clamp(idx + delta, 0, len(qs) - 1)
        if new_idx == idx:
            await self.show_questions(interaction, run_id)
            return

        qs[idx], qs[new_idx] = qs[new_idx], qs[idx]
        for i, q in enumerate(qs):
            q["order"] = i

        # IMPORTANT: update underlying objects already referenced by run["questions"]
        id_to_q = {q["id"]: q for q in run.get("questions", [])}
        for q in qs:
            if q["id"] in id_to_q:
                id_to_q[q["id"]]["order"] = q["order"]

        await self.save_data()
        await self.show_questions(interaction, run_id)

    async def remove_question(self, interaction: discord.Interaction, run_id: str, qid: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="🔒 You can’t remove after submissions open.", ephemeral=True)
            return

        run["questions"] = [q for q in run.get("questions", []) if q.get("id") != qid]
        for i, q in enumerate(sorted(run.get("questions", []), key=lambda x: x.get("order", 0))):
            q["order"] = i

        await self.save_data()
        await self.safe_respond(interaction, content="🗑️ Removed question.", ephemeral=True)
        
    # =====================================================
    # Submissions (open / lock)
    # =====================================================
    async def open_submissions(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "setup_suggestions":
            await self.safe_respond(interaction, content="Submissions can’t be opened right now.", ephemeral=True)
            return
        if not run.get("questions"):
            await self.safe_respond(interaction, content="Add at least one question first.", ephemeral=True)
            return

        run["status"] = "open"
        await self.save_data()

        guild = interaction.guild
        ch_id = run["channels"].get("announcement")
        ch = guild.get_channel(ch_id) if guild else None

        if isinstance(ch, discord.TextChannel):
            msg = await ch.send(
                f"🏁 **{run.get('name','Awards')} are now OPEN!**\n\n"
                "Click below to start or continue your answers:",
                view=PublicEntryView(run_id, suggest=False, fill=True)
            )
            run["public_messages"]["submissions_message_id"] = msg.id
            await self.save_data()

        await self.log_mod(run, "🔓 Submissions opened.")
        await self.safe_respond(interaction, content="✅ Submissions opened.", ephemeral=True)

    async def lock_submissions(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "open":
            await self.safe_respond(interaction, content="Submissions aren’t open.", ephemeral=True)
            return

        run["status"] = "locked"
        await self.save_data()
        await self.log_mod(run, "🔒 Submissions locked.")
        await self.safe_respond(interaction, content="🔒 Submissions locked.", ephemeral=True)

    # =====================================================
    # Fill flow (user answers)
    # =====================================================
    async def start_fill(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        uid = interaction.user.id
        subs = run.setdefault("submissions", {})
        user_sub = subs.setdefault(str(uid), {"answers": {}, "started_at": iso(now_utc()), "submitted_at": None})

        await self.save_data()
        await self.ask_next_question(interaction, run_id, uid)

    async def ask_next_question(self, interaction: discord.Interaction, run_id: str, uid: int):
        run = self.run_by_id(run_id)
        if not run:
            return
        run = normalise_run(run)

        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))
        sub = run.get("submissions", {}).get(str(uid))
        if not sub:
            return

        answers = sub.get("answers", {})
        next_q = next((q for q in qs if q["id"] not in answers), None)

        if not next_q:
            sub["submitted_at"] = iso(now_utc())
            await self.save_data()
            await self.safe_respond(interaction, content="🎉 You’ve completed the awards! Thank you.", ephemeral=True)
            return

        qtype = next_q.get("type")
        qtext = next_q.get("text", "Question")

        if qtype == "short_text":
            await interaction.response.send_modal(
                ShortTextAnswerModal(self, run_id, next_q["id"], qtext)
            )
            return

        if qtype == "user_select":
            max_n = next_q.get("max", 1)
            sel = discord.ui.UserSelect(min_values=1, max_values=max_n, placeholder=qtext)

            async def _cb(i: discord.Interaction):
                ids = [u.id for u in sel.values]
                await self.save_answer(i, run_id, uid, next_q["id"], ids)
                await self.ask_next_question(i, run_id, uid)

            sel.callback = _cb  # type: ignore
            v = discord.ui.View(timeout=300)
            v.add_item(sel)
            await self.safe_respond(interaction, content=f"👉 **{qtext}**", ephemeral=True, view=v)
            return

        if qtype == "multi_choice":
            choices = next_q.get("choices", [])
            opts = [discord.SelectOption(label=c, value=c) for c in choices]
            sel = discord.ui.Select(
                placeholder=qtext,
                min_values=1,
                max_values=1,
                options=opts
            )

            async def _cb(i: discord.Interaction):
                await self.save_answer(i, run_id, uid, next_q["id"], sel.values[0])
                await self.ask_next_question(i, run_id, uid)

            sel.callback = _cb  # type: ignore
            v = discord.ui.View(timeout=300)
            v.add_item(sel)
            await self.safe_respond(interaction, content=f"👉 **{qtext}**", ephemeral=True, view=v)
            return

    async def save_answer(self, interaction: discord.Interaction, run_id: str, uid: int, qid: str, value: Any):
        run = self.run_by_id(run_id)
        if not run:
            return
        run = normalise_run(run)

        sub = run.get("submissions", {}).get(str(uid))
        if not sub:
            return

        sub.setdefault("answers", {})
        sub["answers"][qid] = value
        await self.save_data()

    # =====================================================
    # Reveal + Results
    # =====================================================
    async def reveal(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return
        run = normalise_run(run)

        if run.get("status") != "locked":
            await self.safe_respond(interaction, content="Lock submissions first.", ephemeral=True)
            return

        run["status"] = "reveal"
        run["reveal"]["started_at"] = iso(now_utc())
        await self.save_data()

        await self.post_results(interaction, run_id)

    async def post_results(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            return
        run = normalise_run(run)

        guild = interaction.guild
        ch_id = run["channels"].get("results") or run["channels"].get("announcement")
        ch = guild.get_channel(ch_id) if guild else None
        if not isinstance(ch, discord.TextChannel):
            return

        subs = run.get("submissions", {})
        qs = sorted(run.get("questions", []), key=lambda x: x.get("order", 0))

        await ch.send(f"🏆 **{run.get('name','Awards')} — RESULTS** 🏆")

        for q in qs:
            qid = q["id"]
            qtext = q.get("text", "")
            answers = []
            for s in subs.values():
                if qid in s.get("answers", {}):
                    answers.append(s["answers"][qid])

            em = discord.Embed(title=qtext)
            if not answers:
                em.description = "_No answers submitted._"
            else:
                if q["type"] == "user_select":
                    counts: Dict[int, int] = {}
                    for a in answers:
                        for uid in a:
                            counts[uid] = counts.get(uid, 0) + 1
                    lines = [f"<@{uid}> — **{cnt}**" for uid, cnt in sorted(counts.items(), key=lambda x: -x[1])]
                    em.description = "\n".join(lines)
                else:
                    counts: Dict[str, int] = {}
                    for a in answers:
                        counts[str(a)] = counts.get(str(a), 0) + 1
                    lines = [f"**{k}** — {v}" for k, v in sorted(counts.items(), key=lambda x: -x[1])]
                    em.description = "\n".join(lines)

            await ch.send(embed=em)

        await self.send_chaos_post(run)
        await self.archive_run(run_id)

    async def send_chaos_post(self, run: Dict[str, Any]):
        guild = self.get_guild(int(run["guild_id"]))
        if not guild:
            return
        ch_id = run["channels"].get("results") or run["channels"].get("announcement")
        ch = guild.get_channel(ch_id)
        if not isinstance(ch, discord.TextChannel):
            return

        msg = await ch.send(
            "😈 **Want the chaos stats?**\n"
            "Click below to reveal anonymous breakdowns.",
            view=ChaosView(run["id"])
        )
        run["public_messages"]["chaos_message_id"] = msg.id
        await self.save_data()

    async def send_chaos_stats(self, interaction: discord.Interaction, run_id: str):
        run = next((r for r in self.data.get("archive", []) if r.get("id") == run_id), None)
        if not run:
            await self.safe_respond(interaction, content="Chaos stats not available.", ephemeral=True)
            return

        subs = run.get("submissions", {})
        total = len(subs)

        em = discord.Embed(title="😈 Chaos Stats")
        em.add_field(name="Total submissions", value=str(total), inline=False)

        lengths = [len(s.get("answers", {})) for s in subs.values()]
        if lengths:
            em.add_field(name="Avg answers per user", value=f"{sum(lengths)/len(lengths):.2f}", inline=False)

        await self.safe_respond(interaction, embed=em, ephemeral=True)

    # =====================================================
    # Archive / End
    # =====================================================
    async def archive_run(self, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            return

        run["ended_at"] = iso(now_utc())
        self.data.setdefault("archive", []).append(run)
        self.data["active"] = None
        await self.save_data()

    async def end_run_no_reveal(self, interaction: discord.Interaction, run_id: str):
        run = self.run_by_id(run_id)
        if not run:
            await self.safe_respond(interaction, content="No active run.", ephemeral=True)
            return

        run["ended_at"] = iso(now_utc())
        self.data.setdefault("archive", []).append(run)
        self.data["active"] = None
        await self.save_data()
        await self.safe_respond(interaction, content="🏁 Run ended and archived (no reveal).", ephemeral=True)


# =========================================================
# Entrypoint
# =========================================================
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("TOKEN env var not set")

    bot = AwardsBot()
    bot.run(TOKEN)