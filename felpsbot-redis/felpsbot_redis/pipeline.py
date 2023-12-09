from typing import MutableMapping, Optional, Union

import redis.asyncio.client
from loguru import logger
from redis.asyncio.client import ResponseCallbackT
from redis.asyncio.connection import ConnectionPool


class Pipeline(redis.asyncio.client.Pipeline):
    def __init__(
        self,
        connection_pool: ConnectionPool,
        response_callbacks: MutableMapping[str | bytes, ResponseCallbackT],
        transaction: bool,
        shard_hint: str | None,
    ):
        logger.debug(f"Creating pipeline with {transaction=}")
        super().__init__(connection_pool, response_callbacks, transaction, shard_hint)

    async def execute(self, raise_on_error=True):
        logger.debug(f"Executing pipeline")
        resp = await super().execute(raise_on_error=raise_on_error)
        logger.debug(f"Response from pipeline: {resp!r}")
        return resp

    def execute_command(self, *args, **options):
        command_name = args[0]
        logger.debug(f"Adding command {command_name} to pipeline with args {args[1:]}")
        return super().execute_command(*args, **options)
