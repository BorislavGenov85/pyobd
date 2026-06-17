import asyncio
import json
import sys
import time
import csv
import re

from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import obd
import uvicorn

# -----------------------------------------------------------------
# CSV logic
# -----------------------------------------------------------------

vehicle_info = {
    "vin": "unknown",
    "protocol": "unknown",
    "obd_standard": "unknown",
    "pid_count": 0,
    "connected_at": None,
}
latest_values = {}

logging_enabled = False
log_file = None
log_writer = None
current_log_path = None

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

SNAPSHOT_DIR = LOG_DIR / "snapshots"
SESSION_DIR = LOG_DIR / "sessions"

SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
SESSION_DIR.mkdir(parents=True, exist_ok=True)

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
            return UNIT_MAP.get(unit,unit)

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

            print(f"[PID] {cmd.command.decode()} "
                f"{cmd.name} "
                f"{cmd.desc}"
            )

        except Exception:
            continue

    discovered.sort(key=lambda c: (pid_priority(c),c.command))
    return discovered


def build_scheduler(commands):
    items = []

    for cmd in commands:
        interval = FAST_INTERVAL if cmd.name in FAST_PID_NAMES else SLOW_INTERVAL
        items.append({"command": cmd, "interval": interval, "last": 0.0})

    return items


# VIN helpers
def safe_filename(text):
    if not text:
        return "unknown"
    return re.sub(r'[^A-Za-z0-9_-]', '_', text)


async def read_vehicle_info():
    global vehicle_info
    global connection

    try:
        async with obd_lock:
            vin = await asyncio.to_thread(connection.query, obd.commands.VIN)

        if vin and not vin.is_null() and vin.value:
            raw_vin = vin.value
            if isinstance(raw_vin, (bytes, bytearray)):
                vehicle_info["vin"] = raw_vin.decode("utf-8", errors="ignore").strip()
            else:
                vehicle_info["vin"] = str(vin.value).strip()

    except Exception as ex:

        print("[VIN]", ex)


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
            connection = await asyncio.to_thread(obd.OBD, portstr=OBD_PORT, baudrate=OBD_BAUDRATE, fast=False, timeout=2.0)

            if not connection.is_connected():
                print("[OBD] Connection failed")
                await broadcast({"_status": "disconnected"})
                await asyncio.sleep(RECONNECT_DELAY)
                continue

            print("[OBD] Connected")

            supported_commands = await asyncio.to_thread(discover_supported_pids, connection)
            scheduler = build_scheduler(supported_commands)

            vehicle_vin = vehicle_info["vin"]
            try:
                async with obd_lock:
                    vin_resp = await asyncio.to_thread(connection.query, obd.commands.VIN)
                    if vin_resp and not vin_resp.is_null() and vin_resp.value:
                        vehicle_vin = str(vin_resp.value).strip()
                        print(f"[VIN] Successfully read: {vehicle_vin}")
            except Exception as ex:
                print("[VIN ERROR]", ex)

            vehicle_info.update({
                "vin": vehicle_vin,
                "protocol": connection.protocol_name() if hasattr(connection, 'protocol_name') else "unknown",
                "obd_standard": "unknown",
                "pid_count": len(supported_commands), # Вече няма да е 0!
                "connected_at": datetime.now().isoformat(),
            })

            capabilities = []
            for cmd in supported_commands:
                try:
                    response = await asyncio.to_thread(connection.query, cmd)
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

            await broadcast({"_type": "capabilities", "commands": capabilities})
            return

        except Exception as ex:
            print("[OBD] Connect exception:", ex)
            await broadcast({"_status": "disconnected"})
            await asyncio.sleep(RECONNECT_DELAY)


async def obd_loop():
    global connection

    while True:
        if connection is None or not connection.is_connected():
            await connect_obd()

        loop_time_str = datetime.now().isoformat()
        now = time.time()
        values = {}

        for item in scheduler:
            if now - item["last"] < item["interval"]:
                continue

            cmd = item["command"]

            try:
                async with obd_lock:
                    response = await asyncio.to_thread(connection.query, cmd)

                item["last"] = now

                if response.is_null():
                    continue

                val_extracted = extract_value(response)
                unit_extracted = extract_unit(response)

                values[cmd.name] = {
                    "value": val_extracted,
                    "unit": unit_extracted
                }

                latest_values[cmd.name] = {
                    "name": cmd.desc,
                    "value": val_extracted,
                    "unit": unit_extracted,
                    "timestamp": loop_time_str
                }

                if logging_enabled and log_writer:
                    await asyncio.to_thread(
                        log_writer.writerow, [
                            loop_time_str,
                            cmd.name,
                            cmd.desc,
                            val_extracted,
                            unit_extracted,
                        ]
                    )

            except Exception as ex:
                print("[PID ERROR]", cmd.name, ex)

        if values:
            if logging_enabled and log_file:
                await asyncio.to_thread(log_file.flush)

            await broadcast({"_type": "update", "values": values})

        await asyncio.sleep(0.1)

# ---------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(obd_loop())
    yield

app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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

    if connection is None or not connection.is_connected():
        return JSONResponse({"connected": False, "dtc": []})

    try:
        async with obd_lock:
            response = await asyncio.to_thread(connection.query,obd.commands.GET_DTC)

        dtc_list = []

        if not response.is_null() and response.value:
            for code, desc in response.value:
                dtc_list.append({"code": code, "description": desc,"status": "Active"})

        return {"connected": True, "dtc": dtc_list}

    except Exception as ex:
        return JSONResponse(status_code=500, content={"error": str(ex)})


@app.post("/api/dtc/clear")
async def clear_dtc():
    global connection

    if connection is None or not connection.is_connected():
        return JSONResponse(status_code=400, content={"success": False, "message": "OBD not connected"})

    try:
        async with obd_lock:
            await asyncio.to_thread(connection.query,obd.commands.CLEAR_DTC)

        return {"success": True}

    except Exception as ex:
        return JSONResponse(status_code=500, content={"success": False,"error": str(ex)})


@app.get("/api/freeze")
async def get_freeze():
    global connection

    if connection is None or not connection.is_connected():
        return {"connected": False, "supported": False, "data": []}

    try:
        async with obd_lock:
            response = await asyncio.to_thread(connection.query,obd.commands.FREEZE_DTC)

        if response is None or response.value is None:
            return {"connected": True, "supported": False, "data": []}

        return {"connected": True, "supported": True, "freeze_dtc": str(response.value), "data": []}

    except Exception as ex:
        print("[FREEZE]", ex)
        return {"connected": True, "supported": False, "error": str(ex), "data": []}


# csv FastAPI - export snapshot
@app.get("/api/export/current")
async def export_current():
    if not latest_values:
        return JSONResponse(status_code=400, content={"error": "No data available"})

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = SNAPSHOT_DIR / f"{timestamp}_{safe_filename(vehicle_info['vin'])}.csv"

    current_data_snapshot = dict(latest_values)

    def write_snapshot():
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "PID", "Name", "Value", "Unit"])
            for pid, data in current_data_snapshot.items():
                writer.writerow([data["timestamp"], pid, data["name"], data["value"], data["unit"]])

    await asyncio.to_thread(write_snapshot)
    return FileResponse(path=filename, filename=filename.name, media_type="text/csv")


# csv - start session logging
@app.post("/api/logging/start")
async def start_logging():
    global logging_enabled
    global log_file
    global log_writer
    global current_log_path

    if logging_enabled:
        return {"logging": True, "file": str(current_log_path.name) if current_log_path else "unknown"}

    if connection and connection.is_connected():
        await read_vehicle_info()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = SESSION_DIR / f"{timestamp}_{safe_filename(vehicle_info.get('vin', 'unknown'))}.csv"
    current_log_path = filename

    def open_and_prepare_file():
        f = open(current_log_path, "w", newline="", encoding="utf-8")

        vin_val = vehicle_info.get("vin", "unknown")
        proto_val = vehicle_info.get("protocol", "unknown")
        std_val = vehicle_info.get("obd_standard", "unknown")
        pid_val = vehicle_info.get("pid_count", "0")
        conn_val = vehicle_info.get("connected_at", datetime.now().isoformat())

        f.write(f"# VIN: {vin_val}\n")
        f.write(f"# Protocol: {proto_val}\n")
        f.write(f"# OBD Standard: {std_val}\n")
        f.write(f"# PID count: {pid_val}\n")
        f.write(f"# Connected: {conn_val}\n\n")

        return f

    log_file = await asyncio.to_thread(open_and_prepare_file)
    log_writer = csv.writer(log_file)

    await asyncio.to_thread(log_writer.writerow, ["Timestamp", "PID", "Name", "Value", "Unit"])
    await asyncio.to_thread(log_file.flush)

    logging_enabled = True
    return {"logging": True, "file": str(current_log_path.name)}


# csv - stop session logging
@app.post("/api/logging/stop")
async def stop_logging():
    global logging_enabled
    global log_file
    global current_log_path

    logging_enabled = False

    if log_file:
        await asyncio.to_thread(log_file.close)
        log_file = None
        current_log_path = None

    return {"logging": False}

# test
@app.get("/api/tests")
async def get_vehicle_tests():
    if connection is None or not connection.is_connected():
        return JSONResponse(status_code=400, content={"error": "OBD not connected"})

    try:
        async with obd_lock:
            cmd = obd.commands.STATUS
            response = await asyncio.to_thread(connection.query, cmd)

        if response.is_null() or not response.value:
            return {"status": "no_data", "tests": [], "engine_type": "unknown"}

        status_obj = response.value
        tests_list = []

        engine_type = "unknown"
        if hasattr(status_obj, "ignition_type"):

            raw_type = str(status_obj.ignition_type).lower()
            if "spark" in raw_type:
                engine_type = "Бензин (Spark)"
            elif "compression" in raw_type or "diesel" in raw_type:
                engine_type = "Дизел (Compression)"

        for attr_name in dir(status_obj):
            if attr_name.startswith("_") or attr_name in ["MIL", "DTC_CNT", "DTC_count", "ignition_type"]:
                continue

            monitor = getattr(status_obj, attr_name)

            if isinstance(monitor, dict) and "supported" in monitor:
                ui_name = attr_name.upper() + "_MONITORING"
                available = "---"
                complete = "---"

                if monitor.get("supported", False):
                    available = "Available"
                    complete = "Complete" if monitor.get("ready", False) else "Incomplete"

                tests_list.append({"description": ui_name, "available": available, "complete": complete})

        return {"status": "success", "engine_type": engine_type, "tests": tests_list}

    except Exception as ex:
        print("[TESTS API ERROR]", ex)
        return JSONResponse(status_code=500, content={"error": str(ex)})

    
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
