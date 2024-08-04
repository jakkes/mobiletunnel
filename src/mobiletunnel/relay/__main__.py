import tap
import asyncio
import sys


class Args(tap.Tap):
    port: int


async def connect_stdin_stdout():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    return reader, writer


class Relay:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        self._reader = reader
        self._writer = writer

    async def serve(self):
        while True:

            # Read up to 1 MiB at a time.
            data = await self._reader.read(1024 ** 2)

            if not data:
                break

            self._writer.write(data)
            await self._writer.drain()


async def main(args: Args):
    stdin, stdout = await connect_stdin_stdout()
    socketin, socketout = await asyncio.open_connection("localhost", args.port)

    relay1 = Relay(stdin, socketout)
    relay2 = Relay(socketin, stdout)

    await asyncio.gather(relay1.serve(), relay2.serve())


if __name__ == "__main__":
    args = Args(underscores_to_dashes=True).parse_args()
    asyncio.run(main(args))
