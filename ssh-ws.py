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
RESPONSE = (
    b'HTTP/1.1 101 Switching Protocols\r\n'
    b'\r\n'
)
async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except:
        pass
async def handle_client(client_reader, client_writer):
    target_writer = None
    try:
        first_data = await client_reader.read(BUFFER_SIZE)
        if not first_data:
            return
        target_reader, target_writer = await asyncio.open_connection(
            TARGET_IP,
            TARGET_PORT
        )
        sock = target_writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        target_writer.write(first_data)
        await target_writer.drain()
        client_writer.write(RESPONSE)
        await client_writer.drain()
        await asyncio.gather(
            pipe(client_reader, target_writer),
            pipe(target_reader, client_writer)
        )
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
        reuse_port=True
    )
    async with server:
        await server.serve_forever()
if __name__ == '__main__':
    asyncio.run(main())