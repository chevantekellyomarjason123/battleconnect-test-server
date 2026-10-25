"""
BattleConnect online room server — starter MVP.
Rooms and chat are shared between connected clients while this process is running.
Temporary access-key gated test. Not production-ready: no accounts, rate limiting,
persistence, abuse protection, or game networking. Deploy only for controlled tests.
"""
import asyncio
import json
import os
import secrets
import time
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from pydantic import BaseModel, Field

app = FastAPI(title="BattleConnect Room Server", version="0.1.0")
# Test-only shared access key. Must be supplied as a secret at deployment.
ACCESS_KEY = os.environ.get("BATTLECONNECT_ACCESS_KEY", "")
if len(ACCESS_KEY) < 24:
    raise RuntimeError("Set BATTLECONNECT_ACCESS_KEY to a random value at least 24 characters long")


def authorize(request: Request) -> None:
    provided = request.headers.get("x-battleconnect-key", "")
    if not secrets.compare_digest(provided, ACCESS_KEY):
        raise HTTPException(status_code=403, detail="Invalid test access key")

rooms: Dict[str, dict] = {}
connections: Set[WebSocket] = set()
room_members: Dict[str, Set[WebSocket]] = {}
socket_info: Dict[WebSocket, dict] = {}
lock = asyncio.Lock()


class CreateRoom(BaseModel):
    game: str = Field(min_length=1, max_length=100)
    name: str = Field(min_length=1, max_length=80)
    player: str = Field(min_length=1, max_length=32)
    max_players: int = Field(default=8, ge=2, le=64)
    mode: str = Field(default="Casual", max_length=50)
    private: bool = False
    password: str | None = Field(default=None, max_length=100)


def public_room(room: dict) -> dict:
    return {
        "id": room["id"],
        "game": room["game"],
        "name": room["name"],
        "host": room["host"],
        "players": len(room["players"]),
        "max_players": room["max_players"],
        "mode": room["mode"],
        "private": room["private"],
        "created_at": room["created_at"],
    }


async def broadcast_lobby(event: dict):
    dead = []
    for ws in list(connections):
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connections.discard(ws)
        socket_info.pop(ws, None)


async def broadcast_room(room_id: str, event: dict):
    dead = []
    for ws in list(room_members.get(room_id, set())):
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        room_members.get(room_id, set()).discard(ws)
        socket_info.pop(ws, None)


@app.get("/")
async def root():
    return {"service": "BattleConnect Room Server", "status": "online", "version": "0.1.0"}


@app.get("/health")
async def health():
    return {"status": "ok", "rooms": len(rooms), "connected_clients": len(connections)}


@app.get("/rooms")
async def list_rooms(request: Request):
    authorize(request)
    return {"rooms": [public_room(r) for r in rooms.values() if not r["private"]]}


@app.post("/rooms")
async def create_room(body: CreateRoom, request: Request):
    authorize(request)
    room_id = secrets.token_urlsafe(6)
    room = {
        "id": room_id,
        "game": body.game.strip(),
        "name": body.name.strip(),
        "host": body.player.strip(),
        "players": [body.player.strip()],
        "max_players": body.max_players,
        "mode": body.mode.strip() or "Casual",
        "private": body.private,
        "password": body.password or "",
        "created_at": int(time.time()),
    }
    async with lock:
        rooms[room_id] = room
        room_members[room_id] = set()
    await broadcast_lobby({"type": "rooms_updated"})
    return {"room": public_room(room)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    provided = ws.headers.get("x-battleconnect-key", "")
    if not secrets.compare_digest(provided, ACCESS_KEY):
        await ws.close(code=1008)
        return
    await ws.accept()
    connections.add(ws)
    socket_info[ws] = {"player": "Player", "room_id": None}
    await ws.send_json({"type": "welcome", "message": "Connected to BattleConnect server."})
    try:
        while True:
            message = await ws.receive_json()
            kind = message.get("type")
            player = str(message.get("player", "Player")).strip()[:32] or "Player"

            if kind == "list_rooms":
                visible = [public_room(r) for r in rooms.values() if not r["private"]]
                await ws.send_json({"type": "rooms", "rooms": visible})

            elif kind == "join_room":
                room_id = str(message.get("room_id", ""))
                password = str(message.get("password", ""))
                async with lock:
                    room = rooms.get(room_id)
                    if not room:
                        await ws.send_json({"type": "error", "message": "Room not found."})
                        continue
                    if room["private"] and not secrets.compare_digest(room["password"], password):
                        await ws.send_json({"type": "error", "message": "Incorrect room password."})
                        continue
                    if len(room["players"]) >= room["max_players"] and player not in room["players"]:
                        await ws.send_json({"type": "error", "message": "Room is full."})
                        continue
                    if player not in room["players"]:
                        room["players"].append(player)
                    room_members.setdefault(room_id, set()).add(ws)
                    socket_info[ws] = {"player": player, "room_id": room_id}
                await ws.send_json({"type": "joined_room", "room": public_room(room)})
                await broadcast_room(room_id, {"type": "system", "message": f"{player} joined the room."})
                await broadcast_lobby({"type": "rooms_updated"})

            elif kind == "leave_room":
                info = socket_info.get(ws, {})
                room_id = info.get("room_id")
                if room_id:
                    await leave_room(ws, room_id)
                    await ws.send_json({"type": "left_room"})

            elif kind == "chat":
                text = str(message.get("message", "")).strip()
                info = socket_info.get(ws, {})
                room_id = info.get("room_id")
                if not text:
                    continue
                if len(text) > 500:
                    text = text[:500]
                if not room_id:
                    await ws.send_json({"type": "error", "message": "Join a room before chatting."})
                    continue
                await broadcast_room(room_id, {
                    "type": "chat",
                    "player": info.get("player", player),
                    "message": text,
                    "timestamp": int(time.time()),
                })
            else:
                await ws.send_json({"type": "error", "message": "Unknown message type."})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        info = socket_info.get(ws, {})
        room_id = info.get("room_id")
        if room_id:
            await leave_room(ws, room_id)
        connections.discard(ws)
        socket_info.pop(ws, None)


async def leave_room(ws: WebSocket, room_id: str):
    info = socket_info.get(ws, {})
    player = info.get("player", "Player")
    room_members.get(room_id, set()).discard(ws)
    async with lock:
        room = rooms.get(room_id)
        if room and player in room["players"]:
            room["players"].remove(player)
            if not room["players"]:
                rooms.pop(room_id, None)
                room_members.pop(room_id, None)
            elif room["host"] == player:
                room["host"] = room["players"][0]
    socket_info[ws] = {"player": player, "room_id": None}
    if room_id in rooms:
        await broadcast_room(room_id, {"type": "system", "message": f"{player} left the room."})
    await broadcast_lobby({"type": "rooms_updated"})
