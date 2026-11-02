import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import aiohttp
import discord
from redbot.core import Config, checks, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify

log = logging.getLogger("red.twitchclips")

HELIX = "https://api.twitch.tv/helix"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_PURPLE = 0x9146FF


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class TwitchAPIError(Exception):
    pass


class TwitchClips(commands.Cog):
    """Auto-post new Twitch clips to Discord channels."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x7C11B5, force_registration=True)
        self.config.register_global(interval=120, lookback=180)
        # streamers: {login: {"id": str, "display": str, "channel": int,
        #                     "creator": Optional[str], "min_views": int,
        #                     "mention": Optional[str], "seen": {clip_id: created_at}}}
        self.config.register_guild(streamers={}, embed=True)
        self.session = aiohttp.ClientSession()
        self._token: Optional[str] = None
        self._token_exp: datetime = datetime.min.replace(tzinfo=timezone.utc)
        self._game_cache: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        self.task = asyncio.create_task(self._loop())

    async def cog_unload(self):
        self.task.cancel()
        await self.session.close()

    async def red_delete_data_for_user(self, **kwargs):
        return

    # ---------------- Twitch API ----------------

    async def _creds(self):
        tokens = await self.bot.get_shared_api_tokens("twitch")
        cid, secret = tokens.get("client_id"), tokens.get("client_secret")
        if not cid or not secret:
            raise TwitchAPIError(
                "Twitch credentials missing. Use `[p]set api twitch client_id,ID client_secret,SECRET`."
            )
        return cid, secret

    async def _get_token(self, force: bool = False) -> str:
        now = datetime.now(timezone.utc)
        if self._token and not force and now < self._token_exp:
            return self._token
        cid, secret = await self._creds()
        params = {"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"}
        async with self.session.post(TOKEN_URL, params=params) as r:
            data = await r.json()
            if r.status != 200 or "access_token" not in data:
                raise TwitchAPIError(f"Token request failed: {data}")
        self._token = data["access_token"]
        self._token_exp = now + timedelta(seconds=int(data.get("expires_in", 3600)) - 300)
        return self._token

    async def _helix(self, endpoint: str, params) -> dict:
        cid, _ = await self._creds()
        for attempt in range(2):
            token = await self._get_token(force=attempt == 1)
            headers = {"Client-ID": cid, "Authorization": f"Bearer {token}"}
            async with self.session.get(f"{HELIX}/{endpoint}", params=params, headers=headers) as r:
                if r.status == 401 and attempt == 0:
                    continue
                if r.status == 429:
                    reset = r.headers.get("Ratelimit-Reset")
                    wait = max(1, int(reset) - int(datetime.now().timestamp())) if reset else 5
                    await asyncio.sleep(min(wait, 60))
                    continue
                data = await r.json()
                if r.status != 200:
                    raise TwitchAPIError(f"{endpoint} -> {r.status}: {data}")
                return data
        raise TwitchAPIError(f"{endpoint} failed after retry")

    async def _get_user(self, login: str) -> Optional[dict]:
        data = await self._helix("users", {"login": login.lower()})
        return data["data"][0] if data.get("data") else None

    async def _get_clips(self, broadcaster_id: str, since: datetime) -> List[dict]:
        clips, cursor = [], None
        for _ in range(5):  # max 500 clips per poll
            params = {
                "broadcaster_id": broadcaster_id,
                "started_at": _iso(since),
                "ended_at": _iso(datetime.now(timezone.utc) + timedelta(minutes=5)),
                "first": 100,
            }
            if cursor:
                params["after"] = cursor
            data = await self._helix("clips", params)
            clips.extend(data.get("data", []))
            cursor = data.get("pagination", {}).get("cursor")
            if not cursor:
                break
        return clips

    async def _game_name(self, game_id: str) -> Optional[str]:
        if not game_id:
            return None
        if game_id in self._game_cache:
            return self._game_cache[game_id]
        try:
            data = await self._helix("games", {"id": game_id})
            name = data["data"][0]["name"] if data.get("data") else None
        except TwitchAPIError:
            name = None
        if name:
            self._game_cache[game_id] = name
        return name

    # ---------------- Polling ----------------

    async def _loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._check_all()
            except asyncio.CancelledError:
                raise
            except TwitchAPIError as e:
                log.warning("Twitch API error: %s", e)
            except Exception:
                log.exception("Error while checking clips")
            await asyncio.sleep(max(60, await self.config.interval()))

    async def _check_all(self):
        async with self._lock:
            lookback = await self.config.lookback()
            since = datetime.now(timezone.utc) - timedelta(minutes=lookback)
            all_guilds = await self.config.all_guilds()
            clip_cache: Dict[str, List[dict]] = {}
            for gid, gdata in all_guilds.items():
                guild = self.bot.get_guild(gid)
                if not guild or not gdata.get("streamers"):
                    continue
                if await self.bot.cog_disabled_in_guild(self, guild):
                    continue
                await self._check_guild(guild, gdata, since, clip_cache)

    async def _check_guild(self, guild, gdata, since, clip_cache):
        use_embed = gdata.get("embed", True)
        changed = False
        streamers = gdata["streamers"]
        for login, s in streamers.items():
            channel = guild.get_channel(s["channel"])
            if channel is None:
                continue
            if s["id"] not in clip_cache:
                clip_cache[s["id"]] = await self._get_clips(s["id"], since)
            clips = sorted(clip_cache[s["id"]], key=lambda c: c["created_at"])
            seen = s.setdefault("seen", {})
            for clip in clips:
                if clip["id"] in seen:
                    continue
                seen[clip["id"]] = clip["created_at"]
                changed = True
                if s.get("creator") and clip["creator_name"].lower() != s["creator"].lower():
                    continue
                if clip.get("view_count", 0) < s.get("min_views", 0):
                    # don't mark as seen so it can be posted once it hits the threshold
                    del seen[clip["id"]]
                    continue
                await self._post(channel, clip, s.get("mention"), use_embed)
            # prune seen entries older than lookback window + buffer
            cutoff = since - timedelta(hours=1)
            for cid in [k for k, v in seen.items() if _parse(v) < cutoff]:
                del seen[cid]
                changed = True
        if changed:
            await self.config.guild(guild).streamers.set(streamers)

    async def _post(self, channel: discord.TextChannel, clip: dict, mention: Optional[str], use_embed: bool):
        perms = channel.permissions_for(channel.guild.me)
        if not perms.send_messages:
            return
        content = mention or ""
        allowed = discord.AllowedMentions(everyone=True, roles=True, users=False)
        try:
            if use_embed and perms.embed_links:
                game = await self._game_name(clip.get("game_id"))
                e = discord.Embed(
                    title=clip["title"][:256] or "Untitled clip",
                    url=clip["url"],
                    colour=TWITCH_PURPLE,
                    timestamp=_parse(clip["created_at"]),
                )
                e.set_author(
                    name=f"New clip on {clip['broadcaster_name']}",
                    url=f"https://twitch.tv/{clip['broadcaster_name']}",
                )
                e.add_field(name="Clipped by", value=clip["creator_name"] or "Unknown")
                if game:
                    e.add_field(name="Game", value=game)
                e.add_field(name="Duration", value=f"{clip.get('duration', 0):.0f}s")
                if clip.get("thumbnail_url"):
                    e.set_image(url=clip["thumbnail_url"])
                e.set_footer(text="Twitch Clip")
                await channel.send(content=content or None, embed=e, allowed_mentions=allowed)
            else:
                await channel.send(f"{content} {clip['url']}".strip(), allowed_mentions=allowed)
        except discord.HTTPException:
            log.exception("Failed to post clip %s in %s", clip["id"], channel.id)

    # ---------------- Commands ----------------

    @commands.group(name="twitchclips", aliases=["tclips"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def tc(self, ctx: commands.Context):
        """Manage Twitch clip notifications."""

    @tc.command(name="add")
    async def tc_add(self, ctx, twitch_user: str, channel: discord.TextChannel):
        """Watch a Twitch channel and post its new clips to a Discord channel."""
        twitch_user = twitch_user.lower().strip().split("/")[-1]
        try:
            user = await self._get_user(twitch_user)
        except TwitchAPIError as e:
            return await ctx.send(str(e).replace("[p]", ctx.clean_prefix))
        if not user:
            return await ctx.send(f"Twitch user `{twitch_user}` not found.")
        # Pre-seed seen clips so the backlog isn't spammed.
        lookback = await self.config.lookback()
        since = datetime.now(timezone.utc) - timedelta(minutes=lookback)
        existing = await self._get_clips(user["id"], since)
        async with self.config.guild(ctx.guild).streamers() as st:
            prev = st.get(user["login"], {})
            st[user["login"]] = {
                "id": user["id"],
                "display": user["display_name"],
                "channel": channel.id,
                "creator": prev.get("creator"),
                "min_views": prev.get("min_views", 0),
                "mention": prev.get("mention"),
                "seen": {c["id"]: c["created_at"] for c in existing},
            }
        await ctx.send(f"New clips from **{user['display_name']}** will be posted in {channel.mention}.")

    @tc.command(name="remove", aliases=["del", "delete"])
    async def tc_remove(self, ctx, twitch_user: str):
        """Stop watching a Twitch channel."""
        async with self.config.guild(ctx.guild).streamers() as st:
            if st.pop(twitch_user.lower(), None) is None:
                return await ctx.send("That Twitch user isn't being watched.")
        await ctx.send(f"Stopped watching `{twitch_user}`.")

    @tc.command(name="list")
    async def tc_list(self, ctx):
        """List watched Twitch channels."""
        st = await self.config.guild(ctx.guild).streamers()
        if not st:
            return await ctx.send("No Twitch channels are being watched.")
        lines = []
        for login, s in st.items():
            ch = ctx.guild.get_channel(s["channel"])
            extra = []
            if s.get("creator"):
                extra.append(f"creator={s['creator']}")
            if s.get("min_views"):
                extra.append(f"min_views={s['min_views']}")
            if s.get("mention"):
                extra.append("mention set")
            lines.append(f"{s['display']} -> #{ch.name if ch else 'deleted-channel'}"
                         + (f" ({', '.join(extra)})" if extra else ""))
        for page in pagify("\n".join(lines)):
            await ctx.send(box(page))

    @tc.command(name="creator")
    async def tc_creator(self, ctx, twitch_user: str, creator: Optional[str] = None):
        """Only post clips made by a specific clipper. Omit creator to clear."""
        async with self.config.guild(ctx.guild).streamers() as st:
            if twitch_user.lower() not in st:
                return await ctx.send("That Twitch user isn't being watched.")
            st[twitch_user.lower()]["creator"] = creator.lower() if creator else None
        await ctx.send(f"Creator filter {'set to `' + creator + '`' if creator else 'cleared'}.")

    @tc.command(name="minviews")
    async def tc_minviews(self, ctx, twitch_user: str, views: int = 0):
        """Only post clips once they reach a view count (within the lookback window)."""
        async with self.config.guild(ctx.guild).streamers() as st:
            if twitch_user.lower() not in st:
                return await ctx.send("That Twitch user isn't being watched.")
            st[twitch_user.lower()]["min_views"] = max(0, views)
        await ctx.send(f"Minimum views set to {max(0, views)}.")

    @tc.command(name="mention")
    async def tc_mention(self, ctx, twitch_user: str, *, mention: Optional[str] = None):
        """Set text/role mention posted with each clip (e.g. `@Clips`). Omit to clear."""
        if mention:
            role = discord.utils.get(ctx.guild.roles, name=mention.lstrip("@"))
            if role:
                mention = role.mention
        async with self.config.guild(ctx.guild).streamers() as st:
            if twitch_user.lower() not in st:
                return await ctx.send("That Twitch user isn't being watched.")
            st[twitch_user.lower()]["mention"] = mention
        await ctx.send("Mention updated." if mention else "Mention cleared.")

    @tc.command(name="embed")
    async def tc_embed(self, ctx, enabled: bool):
        """Toggle rich embeds (off = plain link, Discord's native preview)."""
        await self.config.guild(ctx.guild).embed.set(enabled)
        await ctx.send(f"Embeds {'enabled' if enabled else 'disabled'}.")

    @tc.command(name="check")
    async def tc_check(self, ctx):
        """Check for new clips now."""
        async with ctx.typing():
            try:
                await self._check_all()
            except TwitchAPIError as e:
                return await ctx.send(str(e).replace("[p]", ctx.clean_prefix))
        await ctx.tick()

    @tc.command(name="test")
    async def tc_test(self, ctx, twitch_user: str):
        """Post the most recent clip (last 7 days) for a watched user to its channel."""
        st = await self.config.guild(ctx.guild).streamers()
        s = st.get(twitch_user.lower())
        if not s:
            return await ctx.send("That Twitch user isn't being watched.")
        channel = ctx.guild.get_channel(s["channel"])
        if not channel:
            return await ctx.send("Target channel no longer exists.")
        clips = await self._get_clips(s["id"], datetime.now(timezone.utc) - timedelta(days=7))
        if not clips:
            return await ctx.send("No clips in the last 7 days.")
        latest = max(clips, key=lambda c: c["created_at"])
        await self._post(channel, latest, s.get("mention"), await self.config.guild(ctx.guild).embed())
        await ctx.tick()

    @tc.command(name="interval")
    @checks.is_owner()
    async def tc_interval(self, ctx, seconds: int):
        """(Owner) Polling interval in seconds (min 60)."""
        await self.config.interval.set(max(60, seconds))
        await ctx.send(f"Polling every {max(60, seconds)}s.")

    @tc.command(name="lookback")
    @checks.is_owner()
    async def tc_lookback(self, ctx, minutes: int):
        """(Owner) How far back each poll searches, in minutes (min 30).

        Twitch's clip index can lag; a wider window catches late clips.
        """
        await self.config.lookback.set(max(30, minutes))
        await ctx.send(f"Lookback set to {max(30, minutes)} minutes.")
