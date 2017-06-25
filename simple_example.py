import functools

import asyncio
from discord.ext import commands
from client import Twytch

bot = commands.Bot(command_prefix=commands.when_mentioned_or('tw.'))

twitch = Twytch()
asyncio.ensure_future(twitch.run())


@bot.command()
async def sub(ctx, *users):
    await twitch.sub_users(users, functools.partial(msg_chan, ctx.channel))
    await ctx.send('Subbed to {}'.format(', '.join(users)))


@bot.command()
async def unsub(ctx, *users):
    for user in users:
        await twitch.unsub_user(user)
        await ctx.send('Unsubbed to {}'.format(user))


async def msg_chan(channel, topic, data):
    user = topic.split('.', 1)[-1]
    if data['type'] == 'stream-up':
        msg = '{user} started streaming at <https://twitch.tv/{user}>'.format(user=user)
    elif data['type'] == 'stream-down':
        msg = '{user} stopped streaming'.format(user=user)
    else:
        return
    await channel.send(msg)


@bot.command()
async def subs(ctx):
    await ctx.send('Subs: {}'.format(len(twitch.subscriptions)))
    await ctx.send('Conns: {}'.format(len(twitch.connections)))


bot.run('token')
