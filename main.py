from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import uuid
import json
import os


app = FastAPI()


# 只允許你的 GitHub Pages 網域（把 YOUR_NAME 換成你的 GitHub 帳號）
ALLOWED_ORIGINS = [
    "https://YOUR_NAME.github.io",
    "http://localhost:5173",  # 本地測試保留
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 健康檢查端點：前端用來喚醒冷啟動中的伺服器
@app.get("/health")
async def health():
    return {"ok": True}


WIN_LINES = [
    [0, 1, 2], [3, 4, 5], [6, 7, 8],
    [0, 3, 6], [1, 4, 7], [2, 5, 8],
    [0, 4, 8], [2, 4, 6],
]


def check_winner(board):
    for line in WIN_LINES:
        a, b, c = line
        if board[a] and board[a] == board[b] == board[c]:
            return {"w": board[a], "l": line}  # 縮短 key
    if all(cell is not None for cell in board):
        return {"w": "draw", "l": None}
    return None


waiting_queue = []
rooms = {}
player_info = {}
queue_lock = asyncio.Lock()


async def send_json(ws: WebSocket, data: dict):
    await ws.send_text(json.dumps(data, separators=(",", ":")))


async def broadcast(room_id: str, data: dict):
    room = rooms.get(room_id)
    if not room:
        return
    for p in room["players"].values():
        await send_json(p, data)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            t = data.get("t")  # 縮短 key：type -> t


            if t == "j":  # join
                async with queue_lock:
                    if waiting_queue:
                        opponent = waiting_queue.pop(0)
                        room_id = uuid.uuid4().hex[:6].upper()
                        rooms[room_id] = {
                            "players": {"X": opponent, "O": ws},
                            "board": [None] * 9,
                            "current": "X",
                            "result": None,
                        }
                        player_info[opponent] = {"room_id": room_id, "symbol": "X"}
                        player_info[ws] = {"room_id": room_id, "symbol": "O"}


                        await send_json(opponent, {
                            "t": "m", "r": room_id, "s": "X", "c": "X",
                        })
                        await send_json(ws, {
                            "t": "m", "r": room_id, "s": "O", "c": "X",
                        })
                    else:
                        waiting_queue.append(ws)
                        await send_json(ws, {"t": "w"})


            elif t == "mv":  # move
                info = player_info.get(ws)
                if not info:
                    continue
                room = rooms.get(info["room_id"])
                if not room or room["result"]:
                    continue
                if room["current"] != info["symbol"]:
                    continue
                idx = data.get("i")
                if not isinstance(idx, int) or idx < 0 or idx > 8:
                    continue
                if room["board"][idx] is not None:
                    continue


                room["board"][idx] = info["symbol"]
                result = check_winner(room["board"])
                if result:
                    room["result"] = result
                else:
                    room["current"] = "O" if info["symbol"] == "X" else "X"


                # 只送「這次下的位置 + 換誰 + 結果」，不送整個棋盤
                await broadcast(info["room_id"], {
                    "t": "u",
                    "i": idx,
                    "p": info["symbol"],
                    "c": room["current"],
                    "r": result,
                })


            elif t == "r":  # reset
                info = player_info.get(ws)
                if not info:
                    continue
                room = rooms.get(info["room_id"])
                if not room:
                    continue
                room["board"] = [None] * 9
                room["current"] = "X"
                room["result"] = None
                await broadcast(info["room_id"], {"t": "rs", "c": "X"})


    except WebSocketDisconnect:
        pass
