import string
import random
import asyncio
from tqdm import tqdm


PACKET_SIZE = 1024 ** 2
SEM = asyncio.Semaphore(100)


async def write(writer: asyncio.StreamWriter, stop: asyncio.Event):
    data = "".join(random.choices(string.ascii_letters, k=PACKET_SIZE)).encode()

    while not stop.is_set():
        await SEM.acquire()
        writer.write(data)
        await writer.drain()


async def read(reader: asyncio.StreamReader, stop: asyncio.Event):
    global RUNNING
    with tqdm(desc="Bytes received") as pbar:
        while not stop.is_set():
            data = await reader.read(PACKET_SIZE)
            if not data:
                stop.set()

            pbar.update(len(data))
            SEM.release()
        print("done ")


async def main():
    reader, writer = await asyncio.open_connection("localhost", 9987)
    stop = asyncio.Event()

    await asyncio.gather(write(writer, stop), read(reader, stop))


if __name__ == "__main__":
    asyncio.run(main())
