import asyncio


async def client_callback(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    print("new connection")
    while True:
        data = await reader.read(1024 ** 2)
        if not data:
            break
        writer.write(data)
        await writer.drain()
        print(data)
    print("connection closed")


async def main():
    await asyncio.start_server(client_callback, "localhost", 1123)
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
