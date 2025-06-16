import asyncio
import tap
import itertools
import datetime
from uuid import UUID

from mobiletunnel.logging import get_logger
from mobiletunnel.common import (
    Constants,
    Connection,
    GenericException,
    normal_operation,
)

LOGGER = get_logger(__name__)

PROTOCOL_VERSION = 0x00

alive_tasks: dict[asyncio.Task, UUID] = {}
alive_connections: dict[UUID, Connection] = {}
alive_processes: dict[UUID, asyncio.subprocess.Process] = {}
dead_tracker: dict[UUID, tuple[datetime.datetime, Connection]] = {}
reestablishing_tasks: dict[asyncio.Task, UUID] = {}


class FatalError(Exception):
    def __init__(self, message: str):
        super().__init__()
        self.message = message


class Args(tap.Tap):

    host: str
    """Host of remote to open a tunnel to."""

    port: int
    """Port of the remote server."""

    local_port: int
    """Local port to forward to the remote server."""

    remote_port: int
    """Remote port that the connections should be forwarded to."""

    path_to_remote_python: str
    """Path to the remote python executable, with mobiletunnel installed."""

    max_connections: int = 1024


async def open_ssh_connection(host: str, port: int, path_to_remote_python: str):
    LOGGER.debug("Starting SSH connection.")
    proc = await asyncio.subprocess.create_subprocess_shell(
        f'ssh -o ConnectTimeout 1 -o ServerAliveInterval 1 {host} "{path_to_remote_python} '
        f'-m mobiletunnel.relay --port {port}"',
        stdin=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    if proc.returncode is not None:
        raise GenericException(f"SSH process exited with code {proc.returncode}.")
    LOGGER.debug("SSH connection established.")

    volatile_reader = proc.stdout
    volatile_writer = proc.stdin

    protocol_bytes = await volatile_reader.readexactly(1)
    LOGGER.debug(f"Received protocol byte: {protocol_bytes}")
    if protocol_bytes[0] != PROTOCOL_VERSION:
        proc.kill()
        await proc.wait()
        raise FatalError("Version mismatch.")

    volatile_writer.write(PROTOCOL_VERSION.to_bytes(1, "big"))
    await volatile_writer.drain()
    LOGGER.debug("Version handshake successful.")

    return proc, volatile_reader, volatile_writer


async def reestablish_connection(connection: Connection, args: Args):

    pushback = 1
    while True:
        try:
            proc, volatile_reader, volatile_writer = await open_ssh_connection(
                args.host, args.port, args.path_to_remote_python
            )
            break
        except FatalError as e:
            LOGGER.error("Failed to reestablish connection. %s", e.message)
            raise e
        except GenericException as e:
            LOGGER.debug(
                "Failed to reestablish connection, retrying in %d seconds. %s",
                pushback,
                e.message,
            )
            await asyncio.sleep(pushback)
            pushback = min(pushback * 2, 30)
            continue
        except Exception:
            LOGGER.debug(
                "Failed to reestablish connection, retrying in %d seconds.", pushback
            )
            await asyncio.sleep(pushback)
            pushback = min(pushback * 2, 30)
            continue

    LOGGER.debug("Reestablishing connection for UUID: %s (%d bytes)", connection.uuid, len(connection.uuid.bytes))
    volatile_writer.write(Constants.OLD_CONNECTION.to_bytes(1, "big"))
    volatile_writer.write(connection.uuid.bytes)
    volatile_writer.write(connection.counter.to_bytes(1, "big"))
    await volatile_writer.drain()

    response_byte = await volatile_reader.readexactly(1)
    if response_byte[0] != Constants.OLD_CONNECTION:
        proc.kill()
        await proc.wait()
        await connection.close(False)
        raise FatalError(
            "Failed to reestablish connection. Invalid response from server."
        )

    sequence_byte = await volatile_reader.readexactly(1)
    sequence = int.from_bytes(sequence_byte, "big")
    if sequence >= Constants.MAX_SEQUENCE_LENGTH or sequence < 0:
        proc.kill()
        await proc.wait()
        await connection.close(False)
        raise FatalError("Failed to reestablish connection. Invalid sequence number.")

    connection.volatile_reader = volatile_reader
    connection.volatile_writer = volatile_writer

    packets_to_resend = await connection.packet_queue.get_nonverified_packets(sequence)
    return proc, packets_to_resend


def connection_callback(args: Args, CONNECTION_DICT_SYNC, NEW_CONNECTION_SEM):
    async def new_connection_callback(
        stable_reader: asyncio.StreamReader, stable_writer: asyncio.StreamWriter
    ):
        async with NEW_CONNECTION_SEM:
            LOGGER.info("New connection received.")

            proc, volatile_reader, volatile_writer = await open_ssh_connection(
                args.host, args.port, args.path_to_remote_python
            )

            volatile_writer.write(Constants.NEW_CONNECTION.to_bytes(1, "big"))
            volatile_writer.write(args.remote_port.to_bytes(2, "big"))
            await volatile_writer.drain()

            uuid_bytes = await volatile_reader.readexactly(16)
            uuid = UUID(bytes=uuid_bytes)

            LOGGER.debug(f"Received UUID: {uuid}")
            volatile_writer.write(uuid_bytes)
            await volatile_writer.drain()

            async with CONNECTION_DICT_SYNC:

                connection = Connection(
                    volatile_reader,
                    volatile_writer,
                    stable_reader,
                    stable_writer,
                    uuid=uuid,
                    max_buffer_size=Constants.MAX_BUFFER_SIZE,
                )

                if len(alive_connections) + len(dead_tracker) >= args.max_connections:
                    await connection.close(True)
                    raise GenericException("Connection limit reached.")

                alive_connections[uuid] = connection
                alive_processes[uuid] = proc
                task = asyncio.create_task(normal_operation(connection))
                alive_tasks[task] = uuid
                CONNECTION_DICT_SYNC.notify()

    return new_connection_callback


async def main(args: Args):
    NEW_CONNECTION_SEM = asyncio.Semaphore(10)
    CONNECTION_DICT_SYNC = asyncio.Condition()

    await asyncio.start_server(
        connection_callback(args, CONNECTION_DICT_SYNC, NEW_CONNECTION_SEM),
        "localhost",
        args.local_port,
    )

    while True:

        LOGGER.debug(
            "Entering main loop step. Currently there are %d alive tasks, "
            "%d alive connections, %d alive processes, %d dead connections "
            "and %d reestablishing tasks.",
            len(alive_tasks),
            len(alive_connections),
            len(alive_processes),
            len(dead_tracker),
            len(reestablishing_tasks),
        )

        async with CONNECTION_DICT_SYNC:

            notify_task = asyncio.create_task(CONNECTION_DICT_SYNC.wait())

            LOGGER.debug("Waiting for ready tasks.")
            done_tasks, _ = await asyncio.wait(
                itertools.chain(
                    [notify_task], alive_tasks.keys(), reestablishing_tasks.keys()
                ),
                return_when=asyncio.FIRST_COMPLETED,
            )
            LOGGER.debug(
                "Main loop about to process done tasks. Top repr are: %s",
                str([repr(task) for task in list(done_tasks)[:3]])
            )

            if notify_task not in done_tasks:
                LOGGER.debug("Notify task not done, cancelling it.")
                notify_task.cancel()
                try:
                    await notify_task
                except asyncio.CancelledError:
                    pass

            LOGGER.debug("Processing %d tasks", len(done_tasks))
            for task in done_tasks:
                if task == notify_task:
                    LOGGER.debug("Processing notify task.")
                    await task
                    continue

                if task in alive_tasks:
                    LOGGER.debug("Processing alive task.")
                    await task
                    uuid = alive_tasks.pop(task)
                    connection = alive_connections.pop(uuid)
                    del alive_processes[uuid]

                    if connection.stable_writer.is_closing():
                        LOGGER.debug("Connection closed, uuid %s.", uuid)
                        await connection.close(True)
                        continue

                    LOGGER.debug("Lost connection, reestablishing for uuid %s.", uuid)
                    dead_tracker[uuid] = (datetime.datetime.now(), connection)
                    reestablishing_task = asyncio.create_task(
                        reestablish_connection(connection, args)
                    )
                    reestablishing_tasks[reestablishing_task] = uuid

                elif task in reestablishing_tasks:
                    LOGGER.debug("Reestablishing task.")

                    uuid = reestablishing_tasks.pop(task)

                    LOGGER.debug("Reestablished connection for uuid %s.", uuid)
                    proc, packets_to_resend = await task
                    _, connection = dead_tracker.pop(uuid)

                    alive_connections[uuid] = connection
                    alive_processes[uuid] = proc
                    alive_task = asyncio.create_task(
                        normal_operation(connection, start_packets=packets_to_resend)
                    )
                    alive_tasks[alive_task] = uuid

                else:
                    LOGGER.error("Unknown task in done tasks.")


if __name__ == "__main__":
    args = Args(underscores_to_dashes=True).parse_args()
    asyncio.run(main(args))
