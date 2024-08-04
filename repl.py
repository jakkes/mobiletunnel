import asyncio


async def main():
    reader, writer = await asyncio.open_connection("localhost", 9987    )
    while True:
        data = input("Enter data: ")
        writer.write(data.encode())
        await writer.drain()

        response = await reader.readexactly(len(data))
        print(response.decode())


if __name__ == "__main__":
    asyncio.run(main())
