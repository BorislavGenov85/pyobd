import asyncio
import json
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import obd
import uvicorn


# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

OBD_PORT = "socket://localhost:35000"
# OBD_PORT = "COM8"

OBD_BAUDRATE = None

FAST_INTERVAL = 1.0
SLOW_INTERVAL = 5.0
RECONNECT_DELAY = 5.0

clients: set[WebSocket] = set()

connection = None
supported_commands = []
scheduler = []
obd_lock = asyncio.Lock()

STATIC_DIR = Path(__file__).parent / "static"


# ---------------------------------------------------------------------
# PID PRIORITY
# ---------------------------------------------------------------------

FAST_PID_NAMES = {
    "RPM",
    "SPEED",
    "THROTTLE_POS",
    "ENGINE_LOAD",
    "MAF",
    "INTAKE_TEMP",
    "COOLANT_TEMP",
    "FUEL_LEVEL",
}

SKIP_COMMANDS = {
    b"0100",
    b"0120",
    b"0140",
    b"0160",
    b"0180",
    b"01A0",
    b"01C0",

    b"0101",
    b"0102",
    b"0103",
    b"0113",
    b"011C",
    b"0141",
    b"0151",
}

UNIT_MAP = {
    "degree_Celsius": "°C",
    "revolutions_per_minute": "rpm",
    "kilometer_per_hour": "km/h",
    "kilopascal": "kPa",
    "pascal": "Pa",
    "volt": "V",
    "millivolt": "mV",
    "ampere": "A",
    "milliampere": "mA",
    "percent": "%",
    "gram_per_second": "g/s",
    "grams_per_second": "g/s",
    "gps": "g/s",
    "second": "s",
    "millisecond": "ms",
    "minute": "min",
    "kilometer": "km",
    "count": "",
    "ratio": "",
    "degree": "°",
    "liter": "L",
    "liter_per_hour": "L/h",
    "lph": "L/h",
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

async def broadcast(data: dict):

    if not clients:
        return

    message = json.dumps(data)

    dead = set()

    for ws in clients:
        try:
            await ws.send_text(message)

        except Exception:
            dead.add(ws)

    clients.difference_update(dead)


def extract_value(response):

    if response is None:
        return None

    if response.is_null():
        return None

    value = response.value

    try:
        if hasattr(value, "magnitude"):
            return round(float(value.magnitude), 2)

        if isinstance(value, (int, float)):
            return value

        return str(value)

    except Exception:
        return str(value)


def extract_unit(response):

    if response is None:
        return ""

    if response.is_null():
        return ""

    value = response.value

    try:
        if hasattr(value, "units"):
            unit = str(value.units)

            return UNIT_MAP.get(
                unit,
                unit
            )
    except Exception:
        pass

    return ""


def pid_priority(command):

    if command.name in FAST_PID_NAMES:
        return 0

    return 1


def discover_supported_pids(conn):

    discovered = []

    for cmd in obd.commands[1]:

        if cmd is None:
            continue

        if cmd.command in SKIP_COMMANDS:
            continue

        try:

            if not conn.supports(cmd):
                continue

            response = conn.query(cmd)

            if response.is_null():
                continue

            discovered.append(cmd)

            print(
                f"[PID] {cmd.command.decode()} "
                f"{cmd.name} "
                f"{cmd.desc}"
            )

        except Exception:
            continue

    discovered.sort(
        key=lambda c: (
            pid_priority(c),
            c.command
        )
    )

    return discovered


def build_scheduler(commands):

    items = []

    for cmd in commands:

        interval = (
            FAST_INTERVAL
            if cmd.name in FAST_PID_NAMES
            else SLOW_INTERVAL
        )

        items.append({
            "command": cmd,
            "interval": interval,
            "last": 0.0,
        })

    return items

# ---------------------------------------------------------------------
# OBD LOOP
# ---------------------------------------------------------------------

async def connect_obd():
    global connection
    global supported_commands
    global scheduler

    while True:

        try:

            print(f"[OBD] Connecting to {OBD_PORT} ...")

            connection = await asyncio.to_thread(
                obd.OBD,
                portstr=OBD_PORT,
                baudrate=OBD_BAUDRATE,
                fast=False,
            )

            if not connection.is_connected():
                print("[OBD] Connection failed")

                await broadcast({
                    "_status": "disconnected"
                })

                await asyncio.sleep(RECONNECT_DELAY)
                continue

            print("[OBD] Connected")

            supported_commands = await asyncio.to_thread(
                discover_supported_pids,
                connection,
            )

            scheduler = build_scheduler(
                supported_commands
            )

            capabilities = []

            for cmd in supported_commands:

                try:

                    response = await asyncio.to_thread(
                        connection.query,
                        cmd
                    )

                    capabilities.append({
                        "name": cmd.name,
                        "desc": cmd.desc,
                        "unit": extract_unit(response),
                        "pid": cmd.command.decode(),
                    })

                except Exception:

                    capabilities.append({
                        "name": cmd.name,
                        "desc": cmd.desc,
                        "unit": "",
                        "pid": cmd.command.decode(),
                    })

            await broadcast({
                "_type": "capabilities",
                "commands": capabilities,
            })

            return

        except Exception as ex:

            print(
                "[OBD] Connect exception:",
                ex
            )

            await broadcast({
                "_status": "disconnected"
            })

            await asyncio.sleep(
                RECONNECT_DELAY
            )

async def obd_loop():

    global connection

    while True:

        if (
                connection is None
                or not connection.is_connected()
        ):
            await connect_obd()

        now = time.time()

        values = {}

        for item in scheduler:

            if (
                    now - item["last"]
                    < item["interval"]
            ):
                continue

            cmd = item["command"]

            try:

                async with obd_lock:

                    response = await asyncio.to_thread(
                        connection.query,
                        cmd
                    )

                item["last"] = now

                if response.is_null():
                    continue

                values[cmd.name] = {
                    "value": extract_value(
                        response
                    ),
                    "unit": extract_unit(
                        response
                    )
                }

            except Exception as ex:

                print(
                    "[PID ERROR]",
                    cmd.name,
                    ex
                )

        if values:
            await broadcast({
                "_type": "update",
                "values": values
            })

        await asyncio.sleep(0.1)

# ---------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(obd_loop())
    yield

app = FastAPI(lifespan=lifespan)

app.mount(
    "/static",
    StaticFiles(directory=STATIC_DIR),
    name="static",
)

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")

@app.websocket("/ws/sensors")
async def sensors_ws(websocket: WebSocket):
    await websocket.accept()

    clients.add(websocket)

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        clients.discard(websocket)

    except Exception:
        clients.discard(websocket)

@app.get("/api/dtc")
async def get_dtc():

    global connection

    if (
            connection is None
            or not connection.is_connected()
    ):
        return JSONResponse({
            "connected": False,
            "dtc": []
        })

    try:

        async with obd_lock:
            # TODO: remove prints
            print("before:", connection.is_connected())

            response = await asyncio.to_thread(
                connection.query,
                obd.commands.GET_DTC
            )
            print("after:", connection.is_connected())
            print("response:", response)

        dtc_list = []

        if (
                not response.is_null()
                and response.value
        ):

            for code, desc in response.value:
                dtc_list.append({
                    "code": code,
                    "description": desc,
                    "status": "Active"
                })

        return {
            "connected": True,
            "dtc": dtc_list
        }

    except Exception as ex:

        return JSONResponse(
            status_code=500,
            content={
                "error": str(ex)
            }
        )

@app.post("/api/dtc/clear")
async def clear_dtc():

    global connection

    if (
            connection is None
            or not connection.is_connected()
    ):
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "message": "OBD not connected"
            }
        )

    try:

        async with obd_lock:

            await asyncio.to_thread(
                connection.query,
                obd.commands.CLEAR_DTC
            )

        return {
            "success": True
        }

    except Exception as ex:

        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(ex)
            }
        )

@app.get("/api/freeze")
async def get_freeze():

    global connection

    if (connection is None or not connection.is_connected()):
        return {
            "connected": False,
            "supported": False,
            "data": []
        }

    try:

        async with obd_lock:

            response = await asyncio.to_thread(
                connection.query,
                obd.commands.FREEZE_DTC
            )

        if (response is None or response.value is None):
            return {
                "connected": True,
                "supported": False,
                "data": []
            }

        return {
            "connected": True,
            "supported": True,
            "freeze_dtc": str(response.value),
            "data": []
        }

    except Exception as ex:

        print("[FREEZE]", ex)

        return {
            "connected": True,
            "supported": False,
            "error": str(ex),
            "data": []
        }

    # ---------------------------------------------------------------------
    # RUN
    # ---------------------------------------------------------------------

    if __name__ == "__main__":
        uvicorn.run(
            app,
            host="127.0.0.1",
            port=8000,
            reload=False,
        )

    # starting with terminal command -> uvicorn webui.main:app --reload
