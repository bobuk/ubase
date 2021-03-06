# μBase

μBase is an oversimplistic key-value database wrapper on top of aiosqlite

## Basic usage

The main idea is simple: to use a preinstalled version of sqlite3 as key-value storage with a simple async interface.

There's only one class, `uBase` that implements methods `get`, `put`, and `keys` to get value by key, to put new value with key, and to scan the database for matching keys, respectively. Because of python nature, you cannot use `uBase` initialization directly, but with `init_db` method

```python-console
>>> import ubase
>>>
>>> DB = await ubase.init_db(":memory:", defaults={"test": "value", "another": ["strange", "value"]}, features={"readed": False})
>>> print(await DB.get("test", default="unknown"))
value

>>> await DB.put("test", "not a value")
>>> print(await DB.get("test"))
not a value

```

Method `init_db` has three main parameters:
 - `filename`: full database filename. You can use `:memory:` for an in-memory database, as always with sqlite3.
 - `defaults`: dictionary with default values which developer must prefill in newly created database. Note that uBase will not rewrite default values if you already created keys in the database before.
 - `features`: additional 'flags' that can be used for matching and selection. It contains the dictionary of features and default values of that. Be careful, only strings, integers, and boolean values for values currently supported.
 - `ignore_existing`: don't raise an exception if the database already exists. True by default.
 
 `uBase.get` is more or less equivalent of `dict.get`, two arguments:
 - `key`: must be string or integer.
 - `default`: value which is returned if `key` is not found.
 
 `uBase.put` used to put any value to a key. If a key is not in the database, it will be created. Otherwise, the new value will be set to the key
 - `key`: string or integer
 - `value`: must be any serializable data type, like `int`, `str`, `dict` or `list` of these types accordingly.
 
 Any other parameters are used as features, which need to be stored with this record. You can skip `value` and use it as `uBase.put(key, feature=value)` if you need only to update some features for the existing value.
 
 `uBase.delete` is obviously to delete unneeded data from the database. The only argument is the `key`.
 
 `ubase.features` is used to get features of the existing key. The only argument is a `key` and the returned value is a `uBaseFeature`, i.e., simple facade for `dict` where the name of features can be used as a simple attribute.
 
 ```python-console
 >>> await DB.put("test", "value", readed = True)
 >>> res = await DB.features("test")
 >>> res.readed
 True
 ```
 
 Usually, you will want slightly more structured keys. Just use colon-separated strings, like "user:bobuk" for example. To add some sugar to these combinations, we have `uBase.<prefix>.<method>`.
 
 ```python-console
 >>> await DB.user.put("umputun", {"firstname":"Eugeny", "lastname": "Doe"})
 >>> print((await DB.user.get("umputun")["firstname"]))
 Eugeny
 >>> print((await DB.get("user:umputun"))["lastname"])
 Doe
 ```
 
 Mighty `uBase.keys` method is very complex and used to find all the items which is greater or lesser than given one. Returned async generator produce tuples contains both key and value, and please note what all items will be in ascending order if `op` oriented to the right (like greater or greater-or-equal sign) or descending order for opposite cases:
 - `op`: operation sign which is used to compare given key and keys in the database, for example ">" or "<=" or members of ubase.OP enum, like ubase.OP.GTE (for Greater or Equal)
 - `key`: the key you will compare with. It must be a string or integer.
 - `mask`: used to limit the searched keys by prefix, for example, "user:". By default, `mask` is empty i.e. no musk applied.
 - `limit`: used to limit the number of answers like in SQL, the default value is -1 i.e. no limits.
 - `bytimestamp`: sometimes you need to find all records in order as it arrives. Set `bytimestamp` to True for that.
 ```python-console
 >>> await DB.user.put(2010, 10)
 >>> await DB.user.put(2011, 8)
 >>> await DB.user.put(2020, 6)
 >>> await DB.user.put(2021, 1)
 >>> async for k,v in DB.keys(OP.GT, 2011, 'user:'):
>>>     print(k)
2020
2021
>>> async for k,v in DB.user.keys("<=", 2020, limit=2)
2020
2011
 ```
 
 If you don't like how I use async generators, feel free to convert it to `list` or use them as regular generators with awesome [aioitertools](https://github.com/omnilib/aioitertools) module.
 
 `uBase.select` is a special way to select only records with features set to some specific value. For example, if you have records where 'unread=True' for messages are not readed by the user.
 
**Note:** all the examples should be tested with ipython, because it's easier to use async/await syntax with it. If you want to use it as a script, see the example below:

## Script example

For more examples, check out tests code at `tests/` folder

```python
import asyncio
import ubase

async def main():
   DB = await ubase.init_db(":memory:") # open or create database
   await DB.put("test:1", 1)            # put 1 into key "test:1"
   await DB.put("test:2", "two")        # put string "two" into key "test:2"
   await DB.put("test:3", "три")        # put string "три" into key "test:3"
   print(await DB.get("test:2"))        # print data from key "test:2" (must be "two", see?)
   await DB.test.put(1, "uno")          # overwrite "test:1" with "uno"
   async for key, value in DB.test.keys(">=", 2):
      print(key, value)                 # print the values for keys "test:2" and "test:3"

if __name__ == '__main__':
   asyncio.run(main())
```

