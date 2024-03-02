import asyncio
from aiohttp import web
import aiomysql
import kristpy
import string
from datetime import datetime
import toml

address_allowed_chars = frozenset(string.ascii_letters + string.digits)
allowed_chars = frozenset(string.ascii_letters + string.digits + "._@")
def is_valid_name(s):
    return set(s) <= allowed_chars
def is_valid_address(s):
    return set(s) <= address_allowed_chars

# Get secrets from a toml file
config = toml.load("/home/herrkatze/carrotpay_config.toml")
user = config["database"]["username"]
passwd = config["database"]["password"]
db = config["database"]["database"]
private_key = config["private_key"]
# Web Server Stuff

routes = web.RouteTableDef()


@routes.get('/names')
@routes.get('/v2/get_names')
async def getNames(request):
    data = request.query
    addr = data['address']
    if not is_valid_name(addr):
        addr = "__CARROTPAY_INVALID"
    names = await getDBNames(addr)
    nameslst = [i[0] for i in names]
    return web.json_response(nameslst)


@routes.get('/address')
@routes.get('/v2/address')
async def getAddress(request):
    data = request.query
    name = data['name']
    if not is_valid_name(name):
        name= "__CARROTPAY_INVALID"
    address = await getDBAddress(name)
    if address:
        return web.Response(text=address[0][0])
    else:
        return web.Response(text="No address with thixs name could be found")
@routes.get('/name_data')
async def getNameData(request):
    data = request.query
    name = data['name']
    ok=False
    if not is_valid_name(name):
        name="__CARROTPAY_INVALID"
    data = await getDBData(name)
    if data:
        data = data[0]
    else:
        data = {}
    if data != {}:
        rsp = {"name": data["name"],
           "owner": data["address"],
           "original_owner": data["original_address"],
           "registered":data["register_date"].isoformat(),
           "updated": data["updated_date"].isoformat()
           }
        if data["transfer_date"]:
            rsp["transferred"] = data["transfer_date"].isoformat()
        if data["metadata"]:
            rsp["a"] = data["metadata"]
        return web.json_response({"ok": True,"name": rsp})
    else:
        return web.json_response({"ok":False,"error":"Name not found"})
@routes.get('/v2/names/{name}')
async def getNameDataV2(request):
    name = request.match_info['name']
    ok=False
    if not is_valid_name(name):
        name="__CARROTPAY_INVALID"
    data = await getDBData(name)
    if data:
        data = data[0]
    else:
        data = {}
    if data != {}:
        rsp = {"name": data["name"],
           "owner": data["address"],
           "original_owner": data["original_address"],
           "registered":data["register_date"].isoformat(),
           "updated": data["updated_date"].isoformat()
           }
        if data["transfer_date"]:
            rsp["transferred"] = data["transfer_date"].isoformat()
        if data["metadata"]:
            rsp["a"] = data["metadata"]
        return web.json_response({"ok": True,"name": rsp})
    else:
        return web.json_response({"ok":False,"error":"Name not found"})


# Database Stuff


wallet: kristpy.wallet


async def setup():
    global wallet
    wallet = await kristpy.wallet.create(private_key)
    return


async def getDBNames(address):
    global loop
    if address == "__CARROTPAY_INVALID": return ()
    async with aiomysql.connect(host='127.0.0.1', port=3306, user=user, password=passwd ,db=db, loop=loop) as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT name FROM carrotpay WHERE address='{address}';")
            return await cur.fetchall()


async def getDBAddress(name):
    if name == "__CARROTPAY_INVALID": return ()
    async with aiomysql.connect(host='127.0.0.1', port=3306, user=user, password=passwd, db=db, loop=loop) as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT address FROM carrotpay WHERE name='{name}';")

            return await cur.fetchall()

async def getDBData(name):
    if name == "__CARROTPAY_INVALID": return {}
    async with aiomysql.connect(host='127.0.0.1', port=3306, user=user, password=passwd, db=db, loop=loop) as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"SELECT * FROM carrotpay WHERE name='{name}';")

            return await cur.fetchall()

async def createDBName(name,address):
    if await getDBAddress(name):
        return False, "name_already_exists"
    async with aiomysql.connect(host='127.0.0.1', port=3306, user=user, password=passwd, db=db, loop=loop,autocommit=True) as conn:
        async with conn.cursor() as cur:
            val= f"INSERT INTO carrotpay (name,address,original_address,register_date,updated_date) VALUES ('{name}','{address}','{address}','{datetime.utcnow()}','{datetime.utcnow()}');"
            await cur.execute(val)
            return True, None

async def transferDBName(name,newAddress):
    async with aiomysql.connect(host='127.0.0.1', port=3306, user=user, password=passwd, db=db, loop=loop,autocommit=True) as conn:
        async with conn.cursor() as cur:
            val= f"UPDATE carrotpay SET address = '{newAddress}' WHERE name = '{name}';"
            await cur.execute(val)



# Krist Stuff


async def handleKristTransactions():
    default = "__KRISTPY_DEFAULT_VALUE"
    global wallet
    async for tx in wallet.ws_transactions.listen():
        if tx.toAddr == wallet.address:
            name = tx.name
            if name and  name.lower() == "carrotpay":
                meta = kristpy.parseCommonMeta(tx.meta)
                if "get_name" in meta and meta["get_name"] != default and is_valid_name(meta["get_name"]) and "@" not in meta["get_name"] and tx.amount >= 10 and tx.fromAddr != "kqxhx5yn9v":
                    if not meta["get_name"].endswith(".crt"):
                        meta["get_name"] += ".crt"
                    success, err = await createDBName(meta["get_name"].lower(),tx.fromAddr)
                    if not success:
                        await wallet.refund(tx, -1, f"error={err}")
                    elif tx.amount > 10:
                        await wallet.refund(tx, tx.amount-10, f"message=You have overpaid for your purchase. You have been refunded {tx.amount-10} Krist")
                elif "get_name" in meta and meta["get_name"] != default and is_valid_name(meta["get_name"]) and tx.fromAddr == "kqxhx5yn9v":
                    await wallet.refund(tx,-1,"error=Due to technical limitations, the SwitchCraft address is not allowed to recieve names, use your own address")
                elif "get_name" in meta and meta["get_name"] != default and tx.amount <10:
                    await wallet.refund(tx, -1, "error=You underpaid for your purchase, This costs 10 krist.")
                elif "get_name" in meta and "katze" in meta["get_name"].lower():
                    await wallet.refund(tx, -1, "error=Name reserved. Contact Herr Katze for approval if you require this name (herrkatze0658 on discord)")
                elif "get_name" in meta and ("@" in meta["get_name"] or not is_valid_name(meta["get_name"])):
                    await wallet.refund(tx,-1,"message=Invalid characters in name, Only A-Z, 0-9 and _ are allowed (.crt may be included or omitted)")
                elif "to" in meta and meta["to"] != default and is_valid_name(meta["to"]):
                    if "@" in meta["to"]:
                        to = meta["to"].split("@")[1]
                    else:
                        to = meta["to"]
                    address = await getDBAddress(to)
                    if address:
                        await wallet.make_transaction(address[0][0],tx.amount,meta["to"] + ";" + tx.meta.replace(f"to={meta['to']};","").replace("carrotpay.kst;",""))
                    else:
                        await wallet.refund(tx,-1,f"error=CarrotPay address {meta['to']} doesn't exist.")
                elif "name" in meta and meta["name"] != default and is_valid_name(meta["name"]) and "@" not in meta["name"] and "transfer_to" in meta and meta["transfer_to"] != default and is_valid_address(meta["transfer_to"]):
                    addr = await getDBAddress(meta["name"])
                    if addr and addr[0][0] == tx.fromAddr and not tx.fromAddr == "kqxhx5yn9v":
                        await transferDBName(meta["name"],meta["transfer_to"])
                        await wallet.refund(tx,-1,f"message=Successfully transferred {meta['name']} to {meta['transfer_to']}")
                    elif addr[0][0] != tx.fromAddr:
                        await wallet.refund(tx,-1,"error=You are not allowed to transfer this name because you do not own it.")
                    elif meta["transfer_to"] == "kqxhx5yn9v":
                        await wallet.refund(tx,-1,"error=Due to technical limitations, the SwitchCraft address is not allowed to recieve names, use your own address")
                    else:
                        await wallet.refund(tx,-1,"error=The name you are trying to transfer doesn't exist. Did you mean to create it?")
                else:
                    await wallet.refund(tx, -1, "error=Missing or invalid command, returning all krist")









loop = asyncio.get_event_loop()
asyncio.run(setup())

app = web.Application()
app.add_routes(routes)


async def main():
    # add stuff to the loop, e.g. using asyncio.create_task()
    ...

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner)
    await site.start()
    asyncio.create_task(setup())
    asyncio.create_task(handleKristTransactions())

    # add more stuff to the loop, if needed

    # wait forever
    await asyncio.Event().wait()


asyncio.run(main())
