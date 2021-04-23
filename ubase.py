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
FeatureType = Union[int, str, bool]


class CantCreateDatabase(Exception):
    pass


class NoOperations(Exception):
    pass


class NotInitialized(Exception):
    pass


class CantFoundKey(Exception):
    pass


class FeatureNotFound(Exception):
    pass


class OP(Enum):
    GT = 1
    GTE = 2
    LT = 9
    LTE = 10


_OP_LIST = {OP.GT: ">", OP.GTE: ">=", OP.LT: "<", OP.LTE: "<="}


class uBaseFeature:
    def __init__(self, ft: dict):
        self.ft = ft

    def __getattr__(self, key: KeyType) -> FeatureType:
        try:
            return self.ft[key]
        except KeyError as k:
            raise AttributeError(k)


class uBaseProxy:
    def __init__(self, base: "uBase", mask: str):
        self.base = base
        self.mask = mask

    async def get(
        self, key: KeyType, default: Optional[ValueType] = None
    ) -> Optional[ValueType]:
        return await self.base.get(f"{self.mask}:{key}", default)

    async def put(self, key: KeyType, data: Optional[ValueType] = None, **args):
        return await self.base.put(f"{self.mask}:{key}", data, **args)

    async def delete(self, key: KeyType):
        return await self.base.delete(f"{self.mask}:{key}")

    async def features(self, key: KeyType) -> Optional[uBaseFeature]:
        return await self.base.features(f"{self.mask}:{key}")

    def select(
        self,
        feature: str,
        target: FeatureType,
        limit: int = -1,
    ) -> AsyncGenerator[Tuple[KeyType, ValueType], None]:
        return self.base.select(feature, target, mask=f"{self.mask}:", limit=limit)

    def keys(
        self,
        op: Union[OP, str],
        key: KeyType,
        bytimestamp: bool = False,
        limit: int = -1,
    ) -> AsyncGenerator[Tuple[KeyType, ValueType], None]:
        return self.base.keys(
            op, f"{self.mask}:{key}", self.mask, bytimestamp=bytimestamp, limit=limit
        )


class uBase:
    def __init__(self, db: str):
        self.dbname = db
        self.db: aiosqlite.Connection
        self.opt_features: Dict = {}

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

    async def features(self, key: KeyType) -> Optional[uBaseFeature]:
        async with self.db.execute(
            "select "
            + ",".join(list(self.opt_features.keys()))
            + f" from kvbase where id = '{key}'"
        ) as cursor:
            res = await cursor.fetchone()
            output = {}
            for n, (k, tp) in enumerate(self.opt_features.items()):
                output[str(k)] = type(tp)(res[n])  # type: ignore
            return uBaseFeature(output)
        return None

    async def put(self, key: KeyType, data: Optional[ValueType] = None, **args):
        if not self.db:
            raise NotInitialized
        dt = json.dumps(data)
        if args:
            irq_k = []
            irq_v = []
            for k, v in args.items():
                if k not in self.opt_features:
                    raise FeatureNotFound(k)
                irq_k.append(k)
                ftype = type(self.opt_features[k])
                if ftype == bool:
                    irq_v.append("0" if not v else "1")
                elif ftype == str:
                    irq_v.append(f"'{v}'")
                elif ftype == int:
                    irq_v.append(str(int(v)))
            ins_k = ", " + ", ".join(irq_k)
            ins_v = ", " + ", ".join(irq_v)
            set_kv = ""
            for k, v in zip(irq_k, irq_v):
                set_kv += f", {k}={v}"
        else:
            ins_k = ins_v = set_kv = ""
        if data is not None:
            await self.db.execute(
                f"INSERT INTO kvbase(id, data {ins_k}) VALUES ('{key}', ? {ins_v}) "
                + f"ON CONFLICT(id) DO UPDATE SET data=? {set_kv}",
                (dt, dt),
            )
        else:
            await self.db.execute(f"UPDATE kvbase SET {set_kv[1:]} WHERE id=?", (key,))

    async def delete(self, key: KeyType):
        if not self.db:
            raise NotInitialized
        await self.db.execute("DELETE FROM kvbase WHERE id=?", (key,))

    async def select(
        self,
        feature: str,
        target: FeatureType,
        mask: Optional[KeyType] = "",
        limit: int = -1,
    ) -> AsyncGenerator[Tuple[KeyType, ValueType], None]:
        if not self.db:
            raise NotInitialized
        if feature not in self.opt_features:
            raise FeatureNotFound
        ftype = type(self.opt_features[feature])
        target_w: Union[str, int]
        if ftype == bool:
            target_w = 1 if target else 0
        elif ftype == int:
            if type(target) == int:
                target_w = target
            else:
                raise FeatureNotFound
        elif ftype == str:
            target_w = f"'{target}'"
        async with self.db.execute(
            "SELECT id, data from kvbase where "
            + f"id like '{mask}%' and {feature} = {target_w}  LIMIT {limit}"
        ) as cursor:
            async for key, item in cursor:
                yield key, json.loads(item) if type(item) == "str" else item
        return

    async def keys(
        self,
        op: Union[OP, str],
        key: KeyType,
        mask: Optional[KeyType] = "",
        bytimestamp=False,
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
        start_from = None

        async with self.db.execute(
            "SELECT id, ts FROM kvbase WHERE id = ? LIMIT 1", (key,)
        ) as cursor:
            frm = await cursor.fetchone()
            if frm:
                start_from = frm

        if not start_from:
            raise CantFoundKey

        req = "SELECT id, data FROM kvbase WHERE "
        if bytimestamp:
            req += f"(ts {operator} '{start_from[1]}')"
            order_by = "ts"
        else:
            req += f"(id {operator} '{start_from[0]}')"
            order_by = "id"

        req += f" and (id like '{mask}%') order by {order_by} {order} limit {limit}"
        async with self.db.execute(req) as cursor:
            async for key, item in cursor:
                yield key, json.loads(item) if type(item) == "str" else item
        return

    async def close(self):
        if not self.db:
            raise NotInitialized
        await self.db.close()


async def init_db(
    name: str,
    defaults: Dict[KeyType, ValueType] = {},
    ignore_existing=True,
    features={},
) -> uBase:
    DB = uBase(name)
    DB.db = await aiosqlite.connect(name, isolation_level=None)
    DB.opt_features = features
    ftype = []
    for k, v in features.items():
        opt_type = type(v)
        if opt_type == bool:
            ftype.append(k + " BOOLEAN DEFAULT " + str(1 if v else 0))
        elif opt_type == int:
            ftype.append(k + " INTEGER DEFAULT " + str(v))
        elif opt_type == str:
            ftype.append(k + " varchar DEFAULT '" + str(v) + "'")
    fres = ("," + (", ".join(ftype))) if ftype else ""

    try:
        create_req = (
            "CREATE TABLE kvbase "
            + "(id varchar(32) PRIMARY KEY UNIQUE, data json, ts int DEFAULT ((julianday('now') - 2440587.5)*86400000) NOT NULL "
            + fres
            + ")"
        )
        await DB.db.execute(create_req)
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
