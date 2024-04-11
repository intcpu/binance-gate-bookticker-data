import asyncio
import websockets
import json
background_tasks = set()


# 异步获取接口返回值
async def subscribe(url, params, open_timeout=5, close_timeout=0.01):
    async with websockets.connect(url, open_timeout=open_timeout, close_timeout=close_timeout, ping_interval=15) as websocket:
        for param in params:
            task = asyncio.create_task(websocket.send(json.dumps(param)))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
        async for message in websocket:
            yield message
