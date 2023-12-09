from typing import Optional

from loguru import logger
from redis import asyncio as aioredis

from .commands import AsyncRedisModuleCommands
from .pipeline import Pipeline

__all__ = ["Redis"]


class Redis(AsyncRedisModuleCommands, aioredis.Redis):
    connection_pool: aioredis.ConnectionPool

    async def connect(self) -> None:
        """Opens a connection with Redis, raises `redis.exceptions.RedisConnectionError` if not able to connect."""
        logger.info(f"Connecting to Redis")
        if await self.ping():
            logger.info("Connected to Redis")

    async def disconnect(self) -> None:
        """Disconnects from Redis."""
        logger.info("Disconnecting from Redis")
        await self.aclose()

    async def execute_command(self, *args, **options):
        command_name = args[0]
        logger.debug(f"Executing command {command_name} with args {args[1:]}")
        resp = await super().execute_command(*args, **options)
        logger.debug(f"Response from command {command_name} with args {args[1:]}: {resp!r}")
        return resp

    def pipeline(self, transaction: bool = True, shard_hint: Optional[str] = None) -> "Pipeline":
        logger.debug(f"Creating pipeline with {transaction=}")
        return Pipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)

    @property
    def json(self):
        if not hasattr(self, "__json_module__"):
            self.__json_module__ = super().json()
        return self.__json_module__

    @property
    def ft(self):
        if not hasattr(self, "__ft_module__"):
            self.__ft_module__ = super().ft()
        return self.__ft_module__
