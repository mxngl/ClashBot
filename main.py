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
import asyncio
import sqlite3
import time
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


# =========================================================
# APS AUTH + API
# =========================================================


def build_auth_url() -> str:
    return APS_AUTH_URL + "?" + urlencode({
        "response_type": "code",
        "client_id": APS_CLIENT_ID or "",
        "redirect_uri": APS_REDIRECT_URI or "",
        "scope": APS_SCOPES,
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


async def aps_get_issues(project_id: str, limit: int = 50):
    token = await get_valid_access_token()
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        resp = await client.get(
            f"{APS_ISSUES_BASE}/projects/{project_id}/issues",
            headers={"Authorization": f"Bearer {token}"},
            params={"limit": limit},
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


@app.get("/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        return {"ok": False, "error": "missing_code"}

    token = await aps_exchange_code_for_token(code)
    expires_at = int(time.time()) + int(token.get("expires_in", 3600))
    db_set_tokens(token["access_token"], token["refresh_token"], expires_at)
    log.info("OAuth tokens stored successfully.")
    return {"ok": True}


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
            "• /clashes mode:(all|closed|my) limit:10\n"
            "• /clash_add issue_id:<id>\n"
            "• /clash_remove issue_id:<id>\n"
            "• /inbox\n"
            "• /overview"
        ),
        inline=False,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


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

    url = build_auth_url()
    await interaction.followup.send(
        f"Open this link to connect ACC (3-legged OAuth):\n{url}\n\n"
        "After login, ACC will redirect to your callback URL and the bot will store tokens in bot.db.",
        ephemeral=True,
    )


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


@tree.command(name="clashes", description="List ACC Issues (Resolve annotations).")
@app_commands.guilds(GUILD)
@app_commands.choices(mode=MODE_CHOICES)
async def clashes(
    interaction: discord.Interaction,
    mode: app_commands.Choice[str] = None,
    limit: int = 10,
):
    project_id = db_get_setting("APS_PROJECT_ID_ISSUES")
    if not project_id:
        await interaction.response.send_message(
            "⚠️ No project id set. Put APS_PROJECT_ID_ISSUES into your .env.",
            ephemeral=True,
        )
        return

    mode_value = mode.value if mode else "all"
    limit = max(1, min(limit, 20))

    await interaction.response.defer(ephemeral=False)

    try:
        issues = await asyncio.wait_for(aps_get_issues(project_id, limit=200), timeout=25)
    except asyncio.TimeoutError:
        await interaction.followup.send("⚠️ APS request timed out (>25s). Try again.", ephemeral=True)
        return
    except Exception as e:
        await interaction.followup.send(f"⚠️ Failed to fetch issues: `{e}`", ephemeral=True)
        return

    # Backward-compatible: older DB rows may have stored str(interaction.user)
    my_issue_ids = (
        db_list_issues_added_by(str(interaction.user.id))
        | db_list_issues_added_by(str(interaction.user))
    )

    def is_closed(s):
        return (s or "").lower() in ["closed", "resolved"]

    if mode_value == "closed":
        issues = [i for i in issues if is_closed(i.get("status"))]
    elif mode_value == "my":
        issues = [i for i in issues if not is_closed(i.get("status")) and str(i.get("id")) in my_issue_ids]
    else:
        issues = [i for i in issues if not is_closed(i.get("status"))]

    if not issues:
        await interaction.followup.send(f"No issues found for mode **{mode_value}**.")
        return

    embed = discord.Embed(title=f"ACC Issues — {mode_value.upper()}")

    for it in issues[:limit]:
        issue_id = str(it.get("id"))
        title = it.get("title") or "Untitled"
        status = it.get("status") or "—"
        added_by = db_get_channel_issue_added_by(str(interaction.channel_id), issue_id)
        assignee = f"<@{added_by}>" if added_by else "—"

        embed.add_field(
            name=str(title)[:80],
            value=(
                f"ID: `{issue_id}`\n"
                f"Status: **{status}**\n"
                f"Assignee: {assignee}\n"
                f"[Open in ACC]({build_acc_issue_url(project_id, issue_id)})"
            ),
            inline=False,
        )

    await interaction.followup.send(embed=embed)


@tree.command(name="clash_add", description="Save an ACC issue id to this channel's inbox.")
@app_commands.guilds(GUILD)
async def clash_add(interaction: discord.Interaction, issue_id: str):
    db_add_channel_issue(str(interaction.channel_id), issue_id.strip(), str(interaction.user.id))
    await interaction.response.send_message(
        f"✅ Added issue `{issue_id.strip()}` to this channel inbox.",
        ephemeral=True,
    )


@tree.command(name="clash_remove", description="Remove an issue id from this channel's inbox.")
@app_commands.guilds(GUILD)
async def clash_remove(interaction: discord.Interaction, issue_id: str):
    db_remove_channel_issue(str(interaction.channel_id), issue_id.strip())
    await interaction.response.send_message(
        f"✅ Removed issue `{issue_id.strip()}` from this channel inbox.",
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
