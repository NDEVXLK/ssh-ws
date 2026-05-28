#!/usr/bin/python3
import asyncio
import socket
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
    pass
LISTEN_IP = '127.0.0.1'
LISTEN_PORT = 3000
TARGET_IP = '127.0.0.1'
TARGET_PORT = 22
BUFFER_SIZE = 16384
TIMEOUT = 60
RESPONSE = b'HTTP/1.1 101 Switching Protocols\r\n\r\n'
async def pipe(reader, writer):
    try:
        while True:
            data = await asyncio.wait_for(
                reader.read(BUFFER_SIZE),
                timeout=TIMEOUT
            )
            if not data:
                break
            writer.write(data)
            await asyncio.wait_for(
                writer.drain(),
                timeout=TIMEOUT
            )
    except:
        pass
async def handle_client(client_reader, client_writer):
    target_writer = None
    try:
        client_data = await asyncio.wait_for(
            client_reader.read(BUFFER_SIZE),
            timeout=TIMEOUT
        )
        if not client_data:
            return
        target_reader, target_writer = await asyncio.wait_for(
            asyncio.open_connection(
                TARGET_IP,
                TARGET_PORT
            ),
            timeout=TIMEOUT
        )
        sock = target_writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_NODELAY,
                1
            )
            sock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_KEEPALIVE,
                1
            )
        client_writer.write(RESPONSE)
        await asyncio.wait_for(
            client_writer.drain(),
            timeout=TIMEOUT
        )
        task1 = asyncio.create_task(
            pipe(client_reader, target_writer)
        )
        task2 = asyncio.create_task(
            pipe(target_reader, client_writer)
        )
        done, pending = await asyncio.wait(
            [task1, task2],
            return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
    except:
        pass
    finally:
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except:
            pass
        if target_writer:
            try:
                target_writer.close()
                await target_writer.wait_closed()
            except:
                pass
async def main():
    server = await asyncio.start_server(
        handle_client,
        LISTEN_IP,
        LISTEN_PORT,
        reuse_address=True,
        reuse_port=True,
        backlog=socket.SOMAXCONN
    )
    async with server:
        await server.serve_forever()
if __name__ == '__main__':
    asyncio.run(main())