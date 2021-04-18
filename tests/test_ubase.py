#!/usr/bin/env python3

import asyncio
import sys

import pytest

sys.path.append(".")


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_base_keys():
    import ubase

    db = await ubase.init_db(":memory:")
    for i in range(10):
        await db.put(f"base:{i}", i)

    res = []
    async for k, v in db.keys(ubase.OP.GT, "base:5", "base:"):
        res.append(v)
    assert len(res) == 4
    assert res[1] == 7

    res = []
    async for k, v in db.keys(ubase.OP.LTE, "base:5", "base:", limit=4):
        res.append(v)
    assert len(res) == 4
    assert res[-1] == 2

    res = []
    async for k, v in db.keys("<", "base:3", "base:", limit=1):
        res.append(v)
    assert len(res) == 1
    assert res[0] == 2

    await db.close()


@pytest.mark.asyncio
async def test_base_simpe():
    import ubase

    db = await ubase.init_db(":memory:")
    await db.put("test", "t")
    assert "t" == await db.get("test")
    await db.close()


@pytest.mark.asyncio
async def test_base_upsert():
    import ubase

    db = await ubase.init_db(":memory:")
    await db.put("test", "t")
    assert "t" == await db.get("test")
    await db.put("test", 1)
    assert 1 == await db.get("test")
    await db.close()


@pytest.mark.asyncio
async def test_base_defaults():
    import ubase

    db = await ubase.init_db(":memory:", defaults={"tuser": "pass"})
    assert "pass" == await db.get("tuser")
    assert "test" == await db.get("key", "test")
    await db.close()


@pytest.mark.asyncio
async def test_base_proxy():
    import ubase

    db = await ubase.init_db(":memory:")
    await db.area.put("test", "pass")
    assert "pass" == await db.area.get("test")
    assert "pass" == await db.get("area:test")
    await db.close()


@pytest.mark.asyncio
async def test_base_keys_proxy():
    import ubase

    db = await ubase.init_db(":memory:")
    for i in range(10):
        await db.area.put(i, i)

    res = []
    async for k, v in db.area.keys("<=", 5, limit=3):
        res.append(v)
    assert len(res) == 3
    assert res[1] == 4
    await db.close()


@pytest.mark.asyncio
async def test_base_keys_ts_no_ts():
    import ubase

    db = await ubase.init_db(":memory:")
    for i in range(5):
        await asyncio.sleep(0.01)
        await db.area.put(i, i)
    for i in range(9, 4, -1):
        await asyncio.sleep(0.01)
        await db.area.put(i, i)
    res = []
    async for k, v in db.area.keys(">=", 0, bytimestamp=True):
        res.append(v)
    assert res == [0, 1, 2, 3, 4, 9, 8, 7, 6, 5]
    res = []
    async for k, v in db.area.keys("<=", 5, limit=3):
        res.append(v)
    assert len(res) == 3
    assert res[1] == 4

    res = []
    async for k, v in db.area.keys("<=", 5, limit=3, bytimestamp=True):
        res.append(v)
    assert len(res) == 3
    assert res == [5, 6, 7]
    await db.close()


@pytest.mark.asyncio
async def test_base_andnot_del():
    import ubase

    db = await ubase.init_db(":memory:")
    await db.area.put("test", "pass")
    assert await db.area.get("test", None) == "pass"
    await db.area.delete("test")
    assert await db.area.get("test", None) is None

    await db.area.put("test", "pass")
    await db.delete("area:test")
    assert await db.area.get("test", None) is None

    await db.close()
