import redis.asyncio.client
import redis.commands.json

from ..pipeline import Pipeline
from .commands import AsyncJSONCommands

__all__ = ["AsyncJSON", "AsyncJSONCommands"]


class AsyncJSON(AsyncJSONCommands, redis.commands.json.JSON):
    def pipeline(self, transaction=True, shard_hint=None):
        """Creates an async pipeline for the JSON module, that can be used for executing
        JSON commands, as well as classic core commands.

        Usage example:

        r = redis.Redis()
        pipe = r.json().pipeline()
        pipe.set('foo', '.', {'hello!': 'world'})
        pipe.get('foo')
        pipe.get('notakey')
        await pipe.execute()
        """
        if isinstance(self.client, redis.RedisCluster):
            return super().pipeline(transaction, shard_hint)
        else:
            p = AsyncPipeline(
                connection_pool=self.client.connection_pool,
                response_callbacks=self._MODULE_CALLBACKS,  # type: ignore
                transaction=transaction,
                shard_hint=shard_hint,
            )

        p._encode = self._encode
        p._decode = self._decode
        return p


class AsyncPipeline(AsyncJSONCommands, Pipeline):
    """Async Pipeline for the JSON module."""

    pass
