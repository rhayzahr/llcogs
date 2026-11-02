from .twitchclips import TwitchClips

__red_end_user_data_statement__ = (
    "This cog stores Twitch channel names and Discord channel IDs configured per server. "
    "It does not store personal user data."
)


async def setup(bot):
    await bot.add_cog(TwitchClips(bot))
