from json import JSONDecoder, JSONEncoder

import redis.commands.redismodules

from .async_json import AsyncJSON


class AsyncRedisModuleCommands(redis.commands.redismodules.AsyncRedisModuleCommands):
    def json(self, encoder=JSONEncoder(), decoder=JSONDecoder()):
        """Access the json namespace, providing support for redis json."""

        return AsyncJSON(client=self, encoder=encoder, decoder=decoder)
