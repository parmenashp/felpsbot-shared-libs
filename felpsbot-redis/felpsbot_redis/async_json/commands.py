import json
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from redis.commands.json._util import JsonType
from redis.commands.json.decoders import decode_dict_keys
from redis.commands.json.path import Path
from redis.exceptions import DataError


class AsyncJSONCommands:
    """Asynchronous JSON commands for Redis."""

    if TYPE_CHECKING:
        # The following methods are abstract and will be implemented
        # by other classes which inherit from this one.
        pipeline = cast(Callable[..., Any], None)
        execute_command = cast(Callable[..., Any], None)
        _encode = cast(Callable[..., Any], None)
        _decode = cast(Callable[..., Any], None)

    async def arrappend(
        self, name: str, path: Optional[str] = Path.root_path(), *args: List[JsonType]
    ) -> List[Union[int, None]]:
        """Asynchronously append the objects `args` to the array under the
        `path` in key `name`.

        For more information see `JSON.ARRAPPEND <https://redis.io/commands/json.arrappend>`_.
        """
        pieces = [name, str(path)]
        for o in args:
            pieces.append(self._encode(o))
        return await self.execute_command("JSON.ARRAPPEND", *pieces)

    async def arrindex(
        self, name: str, path: str, scalar: int, start: Optional[int] = None, stop: Optional[int] = None
    ) -> List[Union[int, None]]:
        """Asynchronously return the index of `scalar` in the JSON array under `path` at key
        `name`.

        The search can be limited using the optional inclusive `start`
        and exclusive `stop` indices.

        For more information see `JSON.ARRINDEX <https://redis.io/commands/json.arrindex>`_.
        """
        pieces = [name, str(path), self._encode(scalar)]
        if start is not None:
            pieces.append(start)
            if stop is not None:
                pieces.append(stop)

        return await self.execute_command("JSON.ARRINDEX", *pieces)

    async def arrinsert(self, name: str, path: str, index: int, *args: List[JsonType]) -> List[Union[int, None]]:
        """Asynchronously insert the objects `args` to the array at index `index`
        under the `path` in key `name`.

        For more information see `JSON.ARRINSERT <https://redis.io/commands/json.arrinsert>`_.
        """
        pieces = [name, str(path), index]
        for o in args:
            pieces.append(self._encode(o))
        return await self.execute_command("JSON.ARRINSERT", *pieces)

    async def arrlen(self, name: str, path: Optional[str] = Path.root_path()) -> List[Union[int, None]]:
        """Asynchronously return the length of the array JSON value under `path`
        at key `name`.

        For more information see `JSON.ARRLEN <https://redis.io/commands/json.arrlen>`_.
        """
        return await self.execute_command("JSON.ARRLEN", name, str(path))

    async def arrpop(
        self, name: str, path: Optional[str] = Path.root_path(), index: Optional[int] = -1
    ) -> List[Union[str, None]]:
        """Asynchronously pop the element at `index` in the array JSON value under
        `path` at key `name`.

        For more information see `JSON.ARRPOP <https://redis.io/commands/json.arrpop>`_.
        """
        return await self.execute_command("JSON.ARRPOP", name, str(path), index)

    async def arrtrim(self, name: str, path: str, start: int, stop: int) -> List[Union[int, None]]:
        """Asynchronously trim the array JSON value under `path` at key `name` to the
        inclusive range given by `start` and `stop`.

        For more information see `JSON.ARRTRIM <https://redis.io/commands/json.arrtrim>`_.
        """
        return await self.execute_command("JSON.ARRTRIM", name, str(path), start, stop)

    async def type(self, name: str, path: Optional[str] = Path.root_path()) -> List[str]:
        """Asynchronously get the type of the JSON value under `path` from key `name`.

        For more information see `JSON.TYPE <https://redis.io/commands/json.type>`_.
        """
        return await self.execute_command("JSON.TYPE", name, str(path))

    async def resp(self, name: str, path: Optional[str] = Path.root_path()) -> List:
        """Asynchronously return the JSON value under `path` at key `name`.

        For more information see `JSON.RESP <https://redis.io/commands/json.resp>`_.
        """
        return await self.execute_command("JSON.RESP", name, str(path))

    async def objkeys(self, name: str, path: Optional[str] = Path.root_path()) -> List[Union[List[str], None]]:
        """Asynchronously return the key names in the dictionary JSON value under `path` at
        key `name`.

        For more information see `JSON.OBJKEYS <https://redis.io/commands/json.objkeys>`_.
        """
        return await self.execute_command("JSON.OBJKEYS", name, str(path))

    async def objlen(self, name: str, path: Optional[str] = Path.root_path()) -> int:
        """Asynchronously return the length of the dictionary JSON value under `path` at key
        `name`.

        For more information see `JSON.OBJLEN <https://redis.io/commands/json.objlen>`_.
        """
        return await self.execute_command("JSON.OBJLEN", name, str(path))

    async def numincrby(self, name: str, path: str, number: int) -> str:
        """Asynchronously increment the numeric (integer or floating point) JSON value under
        `path` at key `name` by the provided `number`.

        For more information see `JSON.NUMINCRBY <https://redis.io/commands/json.numincrby>`_.
        """
        return await self.execute_command("JSON.NUMINCRBY", name, str(path), self._encode(number))

    # The `nummultby` method is deprecated, so it's omitted in this async version.

    async def clear(self, name: str, path: Optional[str] = Path.root_path()) -> int:
        """Asynchronously empty arrays and objects (to have zero slots/keys without deleting the
        array/object). Return the count of cleared paths (ignoring non-array and non-objects
        paths).

        For more information see `JSON.CLEAR <https://redis.io/commands/json.clear>`_.
        """
        return await self.execute_command("JSON.CLEAR", name, str(path))

    async def delete(self, key: str, path: Optional[str] = Path.root_path()) -> int:
        """Asynchronously delete the JSON value stored at key `key` under `path`.

        For more information see `JSON.DEL <https://redis.io/commands/json.del>`_.
        """
        return await self.execute_command("JSON.DEL", key, str(path))

    # The `forget` method is an alias for `delete`
    forget = delete

    async def get(self, name: str, *args, no_escape: Optional[bool] = False) -> List[JsonType] | None:
        """Asynchronously get the object stored as a JSON value at key `name`.

        `args` is zero or more paths, and defaults to root path. `no_escape` is a boolean flag to add no_escape option to get non-ascii characters.

        For more information see `JSON.GET <https://redis.io/commands/json.get>`_.
        """
        pieces = [name]
        if no_escape:
            pieces.append("noescape")

        if len(args) == 0:
            pieces.append(Path.root_path())
        else:
            for p in args:
                pieces.append(str(p))

        try:
            return await self.execute_command("JSON.GET", *pieces)
        except TypeError:
            return None

    async def mget(self, keys: List[str], path: str) -> List[JsonType]:
        """Asynchronously get the objects stored as JSON values under `path`. `keys`
        is a list of one or more keys.

        For more information see `JSON.MGET <https://redis.io/commands/json.mget>`_.
        """
        pieces = []
        pieces += keys
        pieces.append(str(path))
        return await self.execute_command("JSON.MGET", *pieces)

    async def set(
        self,
        name: str,
        path: str,
        obj: JsonType,
        nx: Optional[bool] = False,
        xx: Optional[bool] = False,
        ttl: Optional[int] = None,
        decode_keys: Optional[bool] = False,
    ) -> Optional[str]:
        """
        Asynchronously set the JSON value at key ``name`` under the ``path`` to ``obj``.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded with utf-8.

        For more information see `JSON.SET <https://redis.io/commands/json.set>`_.
        """
        if decode_keys:
            obj = decode_dict_keys(obj)

        pieces = [name, str(path), self._encode(obj)]

        if nx and xx:
            raise Exception("nx and xx are mutually exclusive: use one, the other or neither - but not both")
        elif nx:
            pieces.append("NX")
        elif xx:
            pieces.append("XX")

        if ttl is not None and ttl > 0:
            pipe = self.pipeline()
            pipe.execute_command("JSON.SET", *pieces)
            pipe.expire(name, ttl)
            return await pipe.execute()
        else:
            return await self.execute_command("JSON.SET", *pieces)

    async def mset(self, triplets: List[Tuple[str, str, JsonType]]) -> Optional[str]:
        """
        Asynchronously set the JSON value at key ``name`` under the ``path`` to ``obj``
        for one or more keys.

        ``triplets`` is a list of one or more triplets of key, path, value.

        For more information see `JSON.MSET <https://redis.io/commands/json.mset>`_.
        """
        pieces = []
        for triplet in triplets:
            pieces.extend([triplet[0], str(triplet[1]), self._encode(triplet[2])])

        return await self.execute_command("JSON.MSET", *pieces)

    async def merge(self, name: str, path: str, obj: JsonType, decode_keys: Optional[bool] = False) -> Optional[str]:
        """
        Asynchronously merges a given JSON value into matching paths.

        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded with utf-8.

        For more information see `JSON.MERGE <https://redis.io/commands/json.merge>`_.
        """
        if decode_keys:
            obj = decode_dict_keys(obj)

        pieces = [name, str(path), self._encode(obj)]

        return await self.execute_command("JSON.MERGE", *pieces)

    async def set_file(
        self,
        name: str,
        path: str,
        file_name: str,
        nx: Optional[bool] = False,
        xx: Optional[bool] = False,
        decode_keys: Optional[bool] = False,
    ) -> Optional[str]:
        """
        Asynchronously set the JSON value at key ``name`` under the ``path`` to the content
        of the json file ``file_name``.

        For more information see `JSON.SET <https://redis.io/commands/json.set>`_.
        """
        with open(file_name, "r") as fp:
            file_content = json.loads(fp.read())

        return await self.set(name, path, file_content, nx=nx, xx=xx, decode_keys=decode_keys)

    async def set_path(
        self,
        json_path: str,
        root_folder: str,
        nx: Optional[bool] = False,
        xx: Optional[bool] = False,
        decode_keys: Optional[bool] = False,
    ) -> Dict[str, bool]:
        """
        Asynchronously iterate over ``root_folder`` and set each JSON file to a value
        under ``json_path`` with the file name as the key.

        For more information see `JSON.SET <https://redis.io/commands/json.set>`_.
        """
        set_files_result = {}
        for root, dirs, files in os.walk(root_folder):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    file_name = file_path.rsplit(".")[0]
                    await self.set_file(
                        file_name,
                        json_path,
                        file_path,
                        nx=nx,
                        xx=xx,
                        decode_keys=decode_keys,
                    )
                    set_files_result[file_path] = True
                except json.JSONDecodeError:
                    set_files_result[file_path] = False

        return set_files_result

    async def strlen(self, name: str, path: Optional[str] = None) -> List[Union[int, None]]:
        """Asynchronously return the length of the string JSON value under ``path`` at key
        ``name``.

        For more information see `JSON.STRLEN <https://redis.io/commands/json.strlen>`_.
        """
        pieces = [name]
        if path is not None:
            pieces.append(str(path))
        return await self.execute_command("JSON.STRLEN", *pieces)

    async def toggle(self, name: str, path: Optional[str] = Path.root_path()) -> Union[bool, List[Optional[int]]]:
        """Asynchronously toggle the boolean value under ``path`` at key ``name``,
        returning the new value.

        For more information see `JSON.TOGGLE <https://redis.io/commands/json.toggle>`_.
        """
        return await self.execute_command("JSON.TOGGLE", name, str(path))

    async def strappend(
        self, name: str, value: str, path: Optional[str] = Path.root_path()
    ) -> Union[int, List[Optional[int]]]:
        """Asynchronously append to the string JSON value.

        For more information see `JSON.STRAPPEND <https://redis.io/commands/json.strappend>`_.
        """
        pieces = [name, str(path), self._encode(value)]
        return await self.execute_command("JSON.STRAPPEND", *pieces)

    async def debug(
        self, subcommand: str, key: Optional[str] = None, path: Optional[str] = Path.root_path()
    ) -> Union[int, List[str]]:
        """Asynchronously return the memory usage in bytes of a value under ``path`` from
        key ``name``.

        For more information see `JSON.DEBUG <https://redis.io/commands/json.debug>`_.
        """
        valid_subcommands = ["MEMORY", "HELP"]
        if subcommand not in valid_subcommands:
            raise DataError("The only valid subcommands are ", str(valid_subcommands))
        pieces = [subcommand]
        if subcommand == "MEMORY":
            if key is None:
                raise DataError("No key specified")
            pieces.append(key)
            pieces.append(str(path))
        return await self.execute_command("JSON.DEBUG", *pieces)
