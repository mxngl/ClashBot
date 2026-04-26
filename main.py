# main.py — ClashBot
# Features:
# - Autodesk APS (3-legged OAuth) + token refresh persisted to SQLite
# - FastAPI OAuth callback server (Uvicorn in background thread)
# - Discord app commands (guild-scoped for instant sync)
# - /overview, /link-acc, /acc_hubs, /acc_projects
# - /clashes (mode: all | closed | my) with multiline output format
# - Channel inbox: /clash_add, /clash_remove, /inbox
# - mode:my shows locally saved clashes added by the requesting Discord user
# - Robust timeouts + error handler
#
# Env vars:
#   DISCORD_TOKEN
#   DISCORD_GUILD_ID
#   APS_CLIENT_ID
#   APS_CLIENT_SECRET
#   APS_REDIRECT_URI   (must point to your FastAPI callback e.g. http://<host>:8001/oauth/callback)
#   APS_PROJECT_ID_ISSUES   (single project id, auto-seeded into DB)
#   RESET_GLOBAL_COMMANDS=1 (optional one-time cleanup)
#   OAUTH_HOST (optional, default 0.0.0.0)
#   OAUTH_PORT (optional, default 8001)

import os
import csv
import io
import re
import asyncio
import sqlite3
import time
import secrets
import logging
import contextlib
from contextlib import contextmanager
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
import uvicorn

import discord
from discord import app_commands

# =========================================================
# ENV + CONSTANTS
# =========================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("clashbot")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
APS_CLIENT_ID = os.getenv("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.getenv("APS_CLIENT_SECRET")
APS_REDIRECT_URI = os.getenv("APS_REDIRECT_URI")
APS_PROJECT_ID_ISSUES_DEFAULT = os.getenv("APS_PROJECT_ID_ISSUES")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))

DB_PATH = "bot.db"

# Short-lived OAuth states: {state: (discord_user_id, expires_at)}
_pending_states: dict[str, tuple[str, float]] = {}
# Reference to the Discord bot's event loop, set in run()
_main_loop: asyncio.AbstractEventLoop | None = None

APS_AUTH_URL = "https://developer.api.autodesk.com/authentication/v2/authorize"
APS_TOKEN_URL = "https://developer.api.autodesk.com/authentication/v2/token"
APS_ISSUES_BASE = "https://developer.api.autodesk.com/construction/issues/v1"
APS_DM_BASE = "https://developer.api.autodesk.com/project/v1"

ACC_HOST = "acc.autodesk.com"

APS_SCOPES = "data:read"

MODE_CHOICES = [
    app_commands.Choice(name="all", value="all"),
    app_commands.Choice(name="closed", value="closed"),
    app_commands.Choice(name="my", value="my"),
]

# =========================================================
# DATABASE
# =========================================================


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_init():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                access_token TEXT,
                refresh_token TEXT,
                expires_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS channel_issues (
                channel_id TEXT NOT NULL,
                issue_id TEXT NOT NULL,
                added_by TEXT,
                added_at INTEGER,
                PRIMARY KEY (channel_id, issue_id)
            );

            CREATE TABLE IF NOT EXISTS csv_issues (
                csv_id INTEGER PRIMARY KEY,
                title TEXT,
                status TEXT,
                tags TEXT,
                creator_name TEXT,
                created_date TEXT,
                element_name TEXT,
                element_revit_id TEXT,
                marker_x REAL,
                marker_y REAL,
                marker_z REAL,
                imported_at INTEGER
            );
        """)

    if APS_PROJECT_ID_ISSUES_DEFAULT and not db_get_setting("APS_PROJECT_ID_ISSUES"):
        db_set_setting("APS_PROJECT_ID_ISSUES", APS_PROJECT_ID_ISSUES_DEFAULT)


def db_set_setting(key: str, value: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def db_get_setting(key: str):
    with get_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def db_set_tokens(access_token: str, refresh_token: str, expires_at: int):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO tokens (id, access_token, refresh_token, expires_at) "
            "VALUES (1, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "access_token=excluded.access_token, "
            "refresh_token=excluded.refresh_token, "
            "expires_at=excluded.expires_at",
            (access_token, refresh_token, int(expires_at)),
        )


def db_get_tokens():
    with get_db() as conn:
        row = conn.execute(
            "SELECT access_token, refresh_token, expires_at FROM tokens WHERE id = 1"
        ).fetchone()
    if not row:
        return None
    return {"access_token": row[0], "refresh_token": row[1], "expires_at": int(row[2])}


def db_add_channel_issue(channel_id: str, issue_id: str, added_by: str):
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO channel_issues (channel_id, issue_id, added_by, added_at) "
            "VALUES (?, ?, ?, ?)",
            (str(channel_id), str(issue_id), str(added_by), int(time.time())),
        )


def db_remove_channel_issue(channel_id: str, issue_id: str):
    with get_db() as conn:
        conn.execute(
            "DELETE FROM channel_issues WHERE channel_id = ? AND issue_id = ?",
            (str(channel_id), str(issue_id)),
        )


def db_list_channel_issues(channel_id: str):
    with get_db() as conn:
        return conn.execute(
            "SELECT issue_id, added_by, added_at FROM channel_issues "
            "WHERE channel_id = ? ORDER BY added_at DESC",
            (str(channel_id),),
        ).fetchall()


def db_get_channel_issue_added_by(channel_id: str, issue_id: str):
    with get_db() as conn:
        row = conn.execute(
            "SELECT added_by FROM channel_issues WHERE channel_id = ? AND issue_id = ?",
            (str(channel_id), str(issue_id)),
        ).fetchone()
    return row[0] if row and row[0] else None


def db_list_issues_added_by(added_by: str) -> set:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT issue_id FROM channel_issues WHERE added_by = ?",
            (str(added_by),),
        ).fetchall()
    return {r[0] for r in rows}


def db_upsert_csv_issues(rows: list[dict]) -> int:
    """Insert or replace CSV issues. Returns number of rows processed."""
    def _safe_float(val: str) -> float | None:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    now = int(time.time())
    records = []
    for row in rows:
        element_name = row.get("element name", "")
        m = re.search(r"\[(\d+)\]", element_name)
        element_revit_id = m.group(1) if m else None
        records.append((
            int(row["id"]),
            row.get("text", ""),
            row.get("status", ""),
            row.get("tags", ""),
            row.get("creator name", ""),
            row.get("created date", ""),
            element_name,
            element_revit_id,
            _safe_float(row.get("markerPosition.x")),
            _safe_float(row.get("markerPosition.y")),
            _safe_float(row.get("markerPosition.z")),
            now,
        ))

    with get_db() as conn:
        conn.executemany(
            """
            INSERT INTO csv_issues
                (csv_id, title, status, tags, creator_name, created_date,
                 element_name, element_revit_id, marker_x, marker_y, marker_z, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(csv_id) DO UPDATE SET
                title=excluded.title, status=excluded.status, tags=excluded.tags,
                creator_name=excluded.creator_name, created_date=excluded.created_date,
                element_name=excluded.element_name, element_revit_id=excluded.element_revit_id,
                marker_x=excluded.marker_x, marker_y=excluded.marker_y,
                marker_z=excluded.marker_z, imported_at=excluded.imported_at
            """,
            records,
        )
    return len(records)


def db_get_csv_issues() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT csv_id, title, status, tags, creator_name, element_name, element_revit_id "
            "FROM csv_issues ORDER BY csv_id DESC"
        ).fetchall()
    return [
        {
            "id": str(r[0]),
            "title": r[1] or "Untitled",
            "status": r[2] or "active",
            "tags": r[3] or "",
            "creator_name": r[4] or "",
            "element_name": r[5] or "",
            "element_revit_id": r[6],
            "source": "csv",
        }
        for r in rows
    ]


# =========================================================
# APS AUTH + API
# =========================================================


def create_oauth_state(discord_user_id: str) -> str:
    """Generate a one-time state token tied to a Discord user, valid for 10 minutes."""
    state = secrets.token_urlsafe(24)
    _pending_states[state] = (str(discord_user_id), time.time() + 600)
    return state


def consume_oauth_state(state: str) -> str | None:
    """Return the Discord user ID for a valid state and remove it, or None if invalid/expired."""
    entry = _pending_states.pop(state, None)
    if entry is None:
        return None
    discord_user_id, expires_at = entry
    return discord_user_id if time.time() < expires_at else None


def build_auth_url(state: str) -> str:
    return APS_AUTH_URL + "?" + urlencode({
        "response_type": "code",
        "client_id": APS_CLIENT_ID or "",
        "redirect_uri": APS_REDIRECT_URI or "",
        "scope": APS_SCOPES,
        "state": state,
    })


async def aps_exchange_code_for_token(code: str):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        resp = await client.post(
            APS_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": APS_REDIRECT_URI,
            },
            auth=(APS_CLIENT_ID, APS_CLIENT_SECRET),
        )
        resp.raise_for_status()
        return resp.json()


async def aps_refresh_token(refresh_token: str):
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        resp = await client.post(
            APS_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "scope": APS_SCOPES,
            },
            auth=(APS_CLIENT_ID, APS_CLIENT_SECRET),
        )
        resp.raise_for_status()
        return resp.json()


async def get_valid_access_token() -> str:
    tok = db_get_tokens()
    if not tok:
        raise RuntimeError("Not linked to ACC yet. Run /link-acc.")

    now = int(time.time())
    if tok["expires_at"] > now + 30:
        return tok["access_token"]

    try:
        refreshed = await aps_refresh_token(tok["refresh_token"])
    except httpx.HTTPStatusError as e:
        if e.response is not None and e.response.status_code == 400:
            raise RuntimeError(
                "Your ACC connection needs to be refreshed. Please run /link-acc again to re-authorize the bot."
            ) from e
        raise

    access_token = refreshed["access_token"]
    refresh_token = refreshed.get("refresh_token", tok["refresh_token"])
    expires_at = now + int(refreshed.get("expires_in", 3600))

    db_set_tokens(access_token, refresh_token, expires_at)
    return access_token


async def aps_get_issues(project_id: str, limit: int = 50, status_filter: str | None = None):
    token = await get_valid_access_token()
    params: dict = {"limit": limit}
    if status_filter:
        params["filter[status]"] = status_filter
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        resp = await client.get(
            f"{APS_ISSUES_BASE}/projects/{project_id}/issues",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", data.get("data", []))


async def aps_get_issue(project_id: str, issue_id: str):
    token = await get_valid_access_token()
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        resp = await client.get(
            f"{APS_ISSUES_BASE}/projects/{project_id}/issues/{issue_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        return resp.json()


async def aps_get_hubs():
    token = await get_valid_access_token()
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        resp = await client.get(
            f"{APS_DM_BASE}/hubs",
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        return resp.json().get("data", [])


async def aps_get_projects(hub_id: str):
    token = await get_valid_access_token()
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        resp = await client.get(
            f"{APS_DM_BASE}/hubs/{hub_id}/projects",
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        return resp.json().get("data", [])


def build_acc_issue_url(project_id: str, issue_id: str) -> str:
    return f"https://{ACC_HOST}/build/issues/projects/{project_id}/issues?issueId={issue_id}"


# =========================================================
# FASTAPI (OAuth Callback)
# =========================================================

app = FastAPI()


async def _notify_linked(discord_user_id: str) -> None:
    """DM the user who triggered /link-acc that their ACC is now connected."""
    try:
        user = await client.fetch_user(int(discord_user_id))
        await user.send(
            "✅ **ACC erfolgreich verbunden!**\n"
            "Du kannst den Bot jetzt mit `/clashes`, `/inbox` und `/clash_add` nutzen."
        )
    except Exception as e:
        log.warning("Could not DM user %s after OAuth: %s", discord_user_id, e)


@app.get("/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code:
        return {"ok": False, "error": "missing_code"}

    token = await aps_exchange_code_for_token(code)
    expires_at = int(time.time()) + int(token.get("expires_in", 3600))
    db_set_tokens(token["access_token"], token["refresh_token"], expires_at)
    log.info("OAuth tokens stored successfully.")

    discord_user_id = consume_oauth_state(state) if state else None
    if discord_user_id and _main_loop:
        asyncio.run_coroutine_threadsafe(
            _notify_linked(discord_user_id),
            _main_loop,
        )

    return {"ok": True, "message": "ACC connected. You can close this tab."}


# =========================================================
# DISCORD BOT
# =========================================================

intents = discord.Intents.default()

GUILD = discord.Object(id=GUILD_ID)


class ClashBotClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        try:
            if os.getenv("RESET_GLOBAL_COMMANDS", "0") == "1":
                self.tree.clear_commands(guild=None)
                await self.tree.sync()
                log.info("Cleared GLOBAL commands (RESET_GLOBAL_COMMANDS=1)")

            synced = await self.tree.sync(guild=GUILD)
            log.info("Synced %d commands to guild %d", len(synced), GUILD_ID)
        except Exception as e:
            log.error("Command sync failed: %s", e)


client = ClashBotClient(intents=intents)
tree = client.tree


@tree.command(name="overview", description="Show what this bot can do and how to use it.")
@app_commands.guilds(GUILD)
async def overview(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    tok = db_get_tokens()
    project_id = db_get_setting("APS_PROJECT_ID_ISSUES")

    def flag(ok: bool) -> str:
        return "✅" if ok else "❌"

    embed = discord.Embed(
        title="ClashBot Overview",
        description=(
            "Use this bot to browse ACC Issues and keep a lightweight per-channel inbox.\n"
            "Tip: If commands don't appear immediately after a restart, wait a few seconds for guild sync."
        ),
    )
    embed.add_field(
        name="Status",
        value="\n".join([
            f"ACC linked: {flag(bool(tok))}",
            f"Project set: {flag(bool(project_id))} {f'(`{project_id}`)' if project_id else ''}",
            "Mode `my`: uses clashes you added via `/clash_add`",
        ]),
        inline=False,
    )
    embed.add_field(
        name="Quickstart",
        value=(
            "1) /link-acc → connect Autodesk ACC\n"
            "2) (optional) /acc_hubs + /acc_projects → discover hub/project IDs\n"
            "3) Ensure APS_PROJECT_ID_ISSUES is set in .env (single project setup)\n"
            "4) Use /clash_add in a channel to save relevant issues there\n"
            "Then: /clashes + /inbox"
        ),
        inline=False,
    )
    embed.add_field(
        name="Commands",
        value=(
            "• /link-acc\n"
            "• /acc_hubs\n"
            "• /acc_projects [hub_id]\n"
            "• /import_csv — Resolve CSV importieren\n"
            "• /clashes mode:(all|closed|my) per_page:5\n"
            "• /clash_add issue_id:<id>\n"
            "• /clash_remove issue_id:<id>\n"
            "• /inbox\n"
            "• /overview"
        ),
        inline=False,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


class OAuthView(discord.ui.View):
    def __init__(self, url: str):
        super().__init__()
        self.add_item(discord.ui.Button(
            label="Connect ACC",
            url=url,
            style=discord.ButtonStyle.link,
            emoji="🔗",
        ))


@tree.command(name="link-acc", description="Connect your Autodesk ACC account (OAuth).")
@app_commands.guilds(GUILD)
async def link_acc(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if not (APS_CLIENT_ID and APS_CLIENT_SECRET and APS_REDIRECT_URI):
        await interaction.followup.send(
            "⚠️ Missing APS env vars. Please set APS_CLIENT_ID, APS_CLIENT_SECRET, APS_REDIRECT_URI.",
            ephemeral=True,
        )
        return

    state = create_oauth_state(str(interaction.user.id))
    url = build_auth_url(state)

    embed = discord.Embed(
        title="Connect Autodesk ACC",
        description=(
            "Klick den Button unten um dich mit deinem Autodesk-Konto zu verbinden.\n\n"
            "Nach dem Login bekommst du eine DM von mir zur Bestätigung.\n"
            "Der Link ist **10 Minuten** gültig."
        ),
        color=discord.Color.orange(),
    )
    await interaction.followup.send(embed=embed, view=OAuthView(url), ephemeral=True)


@tree.command(name="acc_hubs", description="List available ACC/BIM360 hubs.")
@app_commands.guilds(GUILD)
async def acc_hubs(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    try:
        hubs = await asyncio.wait_for(aps_get_hubs(), timeout=25)
    except Exception as e:
        await interaction.followup.send(f"⚠️ Failed to fetch hubs: `{e}`", ephemeral=True)
        return

    if not hubs:
        await interaction.followup.send("No hubs found for this user/app.", ephemeral=True)
        return

    embed = discord.Embed(title="ACC Hubs")
    for h in hubs[:10]:
        attrs = h.get("attributes", {})
        name = attrs.get("name") or "(no name)"
        embed.add_field(name=str(name)[:80], value=f"ID: `{h.get('id')}`", inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="acc_projects", description="List projects in a hub (or auto-pick first hub).")
@app_commands.guilds(GUILD)
async def acc_projects(interaction: discord.Interaction, hub_id: str = None):
    await interaction.response.defer(ephemeral=True)

    try:
        if not hub_id:
            hubs = await asyncio.wait_for(aps_get_hubs(), timeout=25)
            if not hubs:
                await interaction.followup.send("No hubs found.", ephemeral=True)
                return
            hub_id = hubs[0].get("id")

        projects = await asyncio.wait_for(aps_get_projects(hub_id), timeout=25)
    except Exception as e:
        await interaction.followup.send(f"⚠️ Failed to fetch projects: `{e}`", ephemeral=True)
        return

    if not projects:
        await interaction.followup.send("No projects found in this hub.", ephemeral=True)
        return

    embed = discord.Embed(title="ACC Projects")
    for p in projects[:10]:
        attrs = p.get("attributes", {})
        name = attrs.get("name") or "(no name)"
        embed.add_field(name=str(name)[:80], value=f"ID: `{p.get('id')}`", inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)


def _is_closed(status: str | None) -> bool:
    return (status or "").lower() in ["closed", "resolved"]


def _normalize_aps_issue(issue: dict) -> dict:
    push = issue.get("pushpinAttributes") or {}
    obj_id = push.get("objectId")
    return {
        "id": str(issue.get("id", "")),
        "title": issue.get("title") or "Untitled",
        "status": issue.get("status") or "—",
        "source": "acc",
        "tags": "",
        "creator_name": "",
        "element_name": "",
        "element_revit_id": str(obj_id) if obj_id else None,
    }


def _merge_and_dedup(csv_issues: list[dict], aps_issues: list[dict]) -> list[dict]:
    """CSV is primary: drop any APS issue whose element_revit_id already exists in CSV."""
    csv_revit_ids = {i["element_revit_id"] for i in csv_issues if i.get("element_revit_id")}
    unique_aps = [
        _normalize_aps_issue(i) for i in aps_issues
        if (_normalize_aps_issue(i)["element_revit_id"] not in csv_revit_ids)
    ]
    return csv_issues + unique_aps


async def _load_issues_for_mode(
    project_id: str, mode_value: str, my_issue_ids: set[str]
) -> list[dict]:
    csv_all = db_get_csv_issues()

    if mode_value == "closed":
        csv_closed = [i for i in csv_all if _is_closed(i["status"])]
        aps_closed = await asyncio.wait_for(
            aps_get_issues(project_id, limit=100, status_filter="closed"),
            timeout=25,
        )
        return _merge_and_dedup(csv_closed, aps_closed)

    if mode_value == "my":
        csv_my = [i for i in csv_all if i["id"] in my_issue_ids and not _is_closed(i["status"])]
        aps_my_ids = my_issue_ids - {i["id"] for i in csv_my}

        async def _safe_fetch(issue_id: str) -> dict | None:
            try:
                return await asyncio.wait_for(aps_get_issue(project_id, issue_id), timeout=10)
            except Exception as e:
                log.warning("Could not fetch issue %s: %s", issue_id, e)
                return None

        if aps_my_ids:
            results = await asyncio.gather(*[_safe_fetch(iid) for iid in aps_my_ids])
            aps_my = [r for r in results if isinstance(r, dict) and not _is_closed(r.get("status"))]
        else:
            aps_my = []

        return _merge_and_dedup(csv_my, aps_my)

    # mode: all — open issues
    csv_open = [i for i in csv_all if not _is_closed(i["status"])]
    aps_all = await asyncio.wait_for(
        aps_get_issues(project_id, limit=100),
        timeout=25,
    )
    aps_open = [i for i in aps_all if not _is_closed(i.get("status"))]
    return _merge_and_dedup(csv_open, aps_open)


_MODE_COLORS = {
    "all":    discord.Color.orange(),
    "my":     discord.Color.blue(),
    "closed": discord.Color.from_rgb(120, 120, 120),
}


def _build_clashes_embed(
    issues: list[dict], page: int, per_page: int, mode_value: str,
    project_id: str, channel_id: str,
) -> discord.Embed:
    total_pages = max(1, (len(issues) + per_page - 1) // per_page)
    page_issues = issues[page * per_page : (page + 1) * per_page]

    embed = discord.Embed(
        title=f"Issues — {mode_value.upper()}",
        description=f"Seite {page + 1}/{total_pages} · {len(issues)} Issues gesamt",
        color=_MODE_COLORS.get(mode_value, discord.Color.orange()),
    )
    for it in page_issues:
        issue_id = it["id"]
        title = it["title"]
        status = it["status"]
        source = it.get("source", "acc")
        tags = it.get("tags", "")
        added_by = db_get_channel_issue_added_by(channel_id, issue_id)
        assignee = f"<@{added_by}>" if added_by else "—"

        source_badge = "**[Resolve]**" if source == "csv" else "**[ACC]**"

        if source == "csv":
            lines = [
                f"ID: `{issue_id}`",
                f"Status: **{status}**",
                f"Tags: {tags}" if tags else None,
                f"Assignee: {assignee}",
            ]
        else:
            lines = [
                f"ID: `{issue_id}`",
                f"Status: **{status}**",
                f"Assignee: {assignee}",
                f"[Open in ACC]({build_acc_issue_url(project_id, issue_id)})",
            ]

        embed.add_field(
            name=f"{source_badge} {str(title)[:75]}",
            value="\n".join(l for l in lines if l is not None),
            inline=False,
        )
    return embed


class ClashesView(discord.ui.View):
    def __init__(
        self,
        issues: list[dict],
        per_page: int,
        mode_value: str,
        project_id: str,
        channel_id: str,
    ):
        super().__init__(timeout=300)
        self.issues = issues
        self.per_page = per_page
        self.mode_value = mode_value
        self.project_id = project_id
        self.channel_id = channel_id
        self.page = 0
        self._sync_buttons()

    @property
    def _total_pages(self) -> int:
        return max(1, (len(self.issues) + self.per_page - 1) // self.per_page)

    def _sync_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self._total_pages - 1

    def _embed(self) -> discord.Embed:
        return _build_clashes_embed(
            self.issues, self.page, self.per_page,
            self.mode_value, self.project_id, self.channel_id,
        )

    @discord.ui.button(label="◀ Zurück", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, _button: discord.ui.Button):
        self.page -= 1
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="Weiter ▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, _button: discord.ui.Button):
        self.page += 1
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def on_timeout(self):
        self.prev_btn.disabled = True
        self.next_btn.disabled = True


@tree.command(name="clashes", description="List ACC Issues (Resolve annotations).")
@app_commands.guilds(GUILD)
@app_commands.choices(mode=MODE_CHOICES)
async def clashes(
    interaction: discord.Interaction,
    mode: app_commands.Choice[str] = None,
    per_page: int = 5,
):
    project_id = db_get_setting("APS_PROJECT_ID_ISSUES")
    if not project_id:
        await interaction.response.send_message(
            "⚠️ No project id set. Put APS_PROJECT_ID_ISSUES into your .env.",
            ephemeral=True,
        )
        return

    mode_value = mode.value if mode else "all"
    per_page = max(1, min(per_page, 10))

    await interaction.response.defer(ephemeral=False)

    # Backward-compatible: older DB rows may have stored str(interaction.user)
    my_issue_ids = (
        db_list_issues_added_by(str(interaction.user.id))
        | db_list_issues_added_by(str(interaction.user))
    )

    try:
        issues = await _load_issues_for_mode(project_id, mode_value, my_issue_ids)
    except asyncio.TimeoutError:
        await interaction.followup.send("⚠️ APS request timed out (>25s). Try again.", ephemeral=True)
        return
    except Exception as e:
        await interaction.followup.send(f"⚠️ Failed to fetch issues: `{e}`", ephemeral=True)
        return

    if not issues:
        await interaction.followup.send(f"Keine Issues gefunden für Modus **{mode_value}**.")
        return

    view = ClashesView(issues, per_page, mode_value, project_id, str(interaction.channel_id))
    await interaction.followup.send(embed=view._embed(), view=view)


async def _autocomplete_all_issues(
    _interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    """Autocomplete from CSV issues (DB, instant) filtered by current input."""
    issues = db_get_csv_issues()
    current_lower = current.lower()
    matches = [
        i for i in issues
        if current_lower in i["title"].lower() or current in i["id"]
    ]
    return [
        app_commands.Choice(name=f"[{i['id']}] {i['title'][:80]}", value=i["id"])
        for i in matches[:25]
    ]


async def _autocomplete_inbox_issues(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    """Autocomplete from issues already in this channel's inbox."""
    rows = db_list_channel_issues(str(interaction.channel_id))
    csv_by_id = {i["id"]: i for i in db_get_csv_issues()}
    current_lower = current.lower()
    choices = []
    for issue_id, _added_by, _added_at in rows:
        title = csv_by_id[issue_id]["title"] if issue_id in csv_by_id else issue_id
        if current_lower in title.lower() or current in issue_id:
            choices.append(app_commands.Choice(name=f"[{issue_id}] {title[:80]}", value=issue_id))
        if len(choices) == 25:
            break
    return choices


@tree.command(name="clash_add", description="Save an issue to this channel's inbox.")
@app_commands.guilds(GUILD)
@app_commands.autocomplete(issue_id=_autocomplete_all_issues)
async def clash_add(interaction: discord.Interaction, issue_id: str):
    db_add_channel_issue(str(interaction.channel_id), issue_id.strip(), str(interaction.user.id))
    await interaction.response.send_message(
        f"✅ Issue `{issue_id.strip()}` zum Channel-Inbox hinzugefügt.",
        ephemeral=True,
    )


@tree.command(name="clash_remove", description="Remove an issue from this channel's inbox.")
@app_commands.guilds(GUILD)
@app_commands.autocomplete(issue_id=_autocomplete_inbox_issues)
async def clash_remove(interaction: discord.Interaction, issue_id: str):
    db_remove_channel_issue(str(interaction.channel_id), issue_id.strip())
    await interaction.response.send_message(
        f"✅ Issue `{issue_id.strip()}` aus dem Channel-Inbox entfernt.",
        ephemeral=True,
    )


@tree.command(name="inbox", description="Show saved issue ids for this channel.")
@app_commands.guilds(GUILD)
async def inbox(interaction: discord.Interaction, limit: int = 10):
    await interaction.response.defer(ephemeral=True)

    project_id = db_get_setting("APS_PROJECT_ID_ISSUES")
    if not project_id:
        await interaction.followup.send(
            "⚠️ No project id set. Put APS_PROJECT_ID_ISSUES into your .env.",
            ephemeral=True,
        )
        return

    limit = max(1, min(limit, 20))
    rows = db_list_channel_issues(str(interaction.channel_id))
    if not rows:
        await interaction.followup.send("Inbox is empty for this channel.", ephemeral=True)
        return

    rows = rows[:limit]

    async def fetch_issue(issue_id: str):
        try:
            return await asyncio.wait_for(aps_get_issue(project_id, issue_id), timeout=10)
        except Exception as e:
            log.warning("Could not fetch issue %s: %s", issue_id, e)
            return None

    issue_data = await asyncio.gather(*[fetch_issue(issue_id) for issue_id, _, _ in rows])

    embed = discord.Embed(title=f"Channel Inbox (last {limit})")

    for (issue_id, added_by, _), data in zip(rows, issue_data):
        if isinstance(data, dict):
            title = data.get("title") or "(no title)"
            status = data.get("status") or "—"
        else:
            title = "(could not load)"
            status = "—"

        embed.add_field(
            name=str(title)[:80],
            value=(
                f"ID: `{issue_id}`\n"
                f"Status: **{status}**\n"
                f"Added by: <@{added_by}>\n"
                f"[Open in ACC]({build_acc_issue_url(project_id, issue_id)})"
            ),
            inline=False,
        )

    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="import_csv", description="Importiere Clashes aus einem Resolve CSV-Export.")
@app_commands.guilds(GUILD)
async def import_csv(interaction: discord.Interaction, file: discord.Attachment):
    if not file.filename.lower().endswith(".csv"):
        await interaction.response.send_message(
            "⚠️ Bitte eine `.csv` Datei hochladen.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    try:
        raw = await file.read()
    except Exception as e:
        await interaction.followup.send(f"⚠️ Datei konnte nicht gelesen werden: `{e}`", ephemeral=True)
        return

    # Try UTF-8 first (with optional BOM), fall back to latin-1
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        await interaction.followup.send("⚠️ Datei-Encoding konnte nicht erkannt werden.", ephemeral=True)
        return

    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = [r for r in reader if r.get("id", "").strip().isdigit()]
    except Exception as e:
        await interaction.followup.send(f"⚠️ CSV konnte nicht geparst werden: `{e}`", ephemeral=True)
        return

    if not rows:
        await interaction.followup.send("⚠️ Keine gültigen Zeilen in der CSV gefunden.", ephemeral=True)
        return

    count = db_upsert_csv_issues(rows)
    log.info("CSV import by %s: %d issues upserted from %s", interaction.user, count, file.filename)
    await interaction.followup.send(
        f"✅ **{count} Issues** aus `{file.filename}` importiert / aktualisiert.\n"
        f"Nutze `/clashes` um sie zu sehen.",
        ephemeral=True,
    )


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    log.error("Command error: %s", error)
    try:
        msg = f"⚠️ Command error: `{error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass


@client.event
async def on_ready():
    log.info("Logged in as %s", client.user)


# =========================================================
# RUN (Discord + OAuth server)
# =========================================================

OAUTH_HOST = os.getenv("OAUTH_HOST", "0.0.0.0")
OAUTH_PORT = int(os.getenv("OAUTH_PORT", "8001"))


def _start_web_server():
    try:
        uvicorn.run(app, host=OAUTH_HOST, port=OAUTH_PORT, log_level="info")
    except OSError as e:
        log.error("Failed to bind %s:%d — %s. OAuth server not started.", OAUTH_HOST, OAUTH_PORT, e)
    except SystemExit as e:
        log.warning("Uvicorn exited: %s. OAuth server not started.", e)


async def run():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    db_init()

    web_task = asyncio.create_task(asyncio.to_thread(_start_web_server))

    try:
        await client.start(DISCORD_TOKEN)
    except asyncio.CancelledError:
        raise
    finally:
        with contextlib.suppress(Exception):
            await client.close()

        if not web_task.done():
            web_task.cancel()
            with contextlib.suppress(Exception):
                await web_task


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
