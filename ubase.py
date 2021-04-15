#!/usr/bin/env python3

import asyncio
from enum import Enum
from typing import AsyncGenerator, Dict, Optional, Tuple, Union

import aiosqlite

try:
    import ujson as json
except ModuleNotFoundError:
    import json  # type: ignore

KeyType = Union[int, str]
ValueType = Union[int, str, dict, list]


class CantCreateDatabase(Exception):
    pass


class NoOperations(Exception):
    pass


class NotInitialized(Exception):
    pass


class OP(Enum):
    GT = 1
    GTE = 2
    LT = 9
    LTE = 10


_OP_LIST = {OP.GT: ">", OP.GTE: ">=", OP.LT: "<", OP.LTE: "<="}


class uBaseProxy:
    def __init__(self, base: "uBase", mask: str):
        self.base = base
        self.mask = mask

    async def get(
        self, key: KeyType, default: Optional[ValueType] = None
    ) -> Optional[ValueType]:
        return await self.base.get(f"{self.mask}:{key}", default)

    async def put(self, key: KeyType, data: ValueType):
        return await self.base.put(f"{self.mask}:{key}", data)

    async def delete(self, key: KeyType):
        return await self.base.delete(f"{self.mask}:{key}")

    def keys(
        self, op: Union[OP, str], key: KeyType, limit: int = -1
    ) -> AsyncGenerator[Tuple[KeyType, ValueType], None]:
        return self.base.keys(op, f"{self.mask}:{key}", self.mask, limit)


class uBase:
    def __init__(self, db: str):
        self.dbname = db
        self.db: aiosqlite.Connection

    def __getattr__(self, mask: str):
        try:
            return uBaseProxy(self, mask)
        except KeyError as k:
            raise AttributeError(k)

    async def get(
        self, key: KeyType, default: Optional[ValueType] = None
    ) -> Optional[ValueType]:
        if not self.db:
            raise NotInitialized
        async with self.db.execute(
            "SELECT data FROM kvbase WHERE id=?", (key,)
        ) as cursor:
            data = await cursor.fetchone()
            if data:
                try:
                    return json.loads(data[0])
                except TypeError:
                    return data[0]
            else:
                return default

    async def put(self, key: KeyType, data: ValueType):
        if not self.db:
            raise NotInitialized
        dt = json.dumps(data)
        await self.db.execute(
            "INSERT INTO kvbase(id, data) VALUES (?,?) "
            + "ON CONFLICT(id) DO UPDATE SET data=?",
            (key, dt, dt),
        )

    async def delete(self, key: KeyType):
        if not self.db:
            raise NotInitialized
        await self.db.execute("DELETE FROM kvbase WHERE id=?", (key,))

    async def keys(
        self,
        op: Union[OP, str],
        key: KeyType,
        mask: Optional[KeyType] = "",
        limit: int = -1,
    ) -> AsyncGenerator[Tuple[KeyType, ValueType], None]:
        if not self.db:
            raise NotInitialized
        operator: str = ""
        if type(op) == str:
            operator = str(op)
        else:
            if op not in _OP_LIST:
                raise NoOperations
            operator = _OP_LIST[OP(op)]
        order = "desc" if "<" in operator else "asc"
        req = (
            "SELECT id, data FROM kvbase WHERE "
            + f"(id {operator} '{key}') and (id LIKE '{mask}%')"
            + f"order by id {order} limit {limit}"
        )
        async with self.db.execute(req) as cursor:
            async for key, item in cursor:
                yield key, json.loads(item) if type(item) == "str" else item
        return

    async def close(self):
        if not self.db:
            raise NotInitialized
        await self.db.close()


async def init_db(
    name: str, defaults: Dict[KeyType, ValueType] = {}, ignore_existing=True
) -> uBase:
    DB = uBase(name)
    DB.db = await aiosqlite.connect(name, isolation_level=None)
    try:
        await DB.db.execute(
            "CREATE TABLE kvbase " + "(id varchar(32) PRIMARY KEY UNIQUE, data json)"
        )
    except aiosqlite.OperationalError:
        if not ignore_existing:
            raise CantCreateDatabase
    for k, v in defaults.items():
        if not await DB.get(k):
            await DB.put(k, v)
    return DB


async def main():
    db = await init_db("test.db")
    await db.put("asdf:0001", "a")
    await db.put("asdf:0002", "b")
    await db.put("asdf:0003", "c")
    await db.put("asdf:0004", "d")
    await db.put("asdf:0005", "e")
    async for key in db.keys(OP.GT, "asdf:0002", "asdf:"):
        print(key)
    print(await db.get("asdf:0003"))

    await db.close()


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
