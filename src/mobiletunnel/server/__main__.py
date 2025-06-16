import tap
import asyncio
import itertools
import datetime
from uuid import uuid4, UUID

from mobiletunnel.logging import get_logger
from mobiletunnel.common import (
    Constants,
    Connection,
    GenericException,
    normal_operation,
    KeyKeyStore
)

LOGGER = get_logger(__name__)

NEW_CONNECTION_SEM = asyncio.Semaphore(10)
CONNECTION_DICT_SYNC = asyncio.Condition()
PROTOCOL_VERSION = 0x00

alive_tasks: KeyKeyStore[asyncio.Task, UUID] = KeyKeyStore()
alive_connections: dict[UUID, Connection] = {}
dead_connections: dict[UUID, tuple[Connection, datetime.datetime]] = {}


class Args(tap.Tap):
    port: int
    max_connections: int = 1024


async def reestablish_normal_operation(connection: Connection):

    LOGGER.debug("Reestablishing normal operation for connection %s", connection.uuid)
    connection.volatile_writer.write(Constants.OLD_CONNECTION)
    connection.volatile_writer.write(connection.counter.to_bytes(1, "big"))
    await connection.volatile_writer.drain()

    sequence_byte = await connection.volatile_reader.readexactly(1)
    sequence = int.from_bytes(sequence_byte, "big")
    if sequence >= Constants.MAX_SEQUENCE_LENGTH or sequence < 0:
        await connection.close(True)
        raise GenericException("Invalid sequence number.")

    await connection.packet_queue.step_verified_counter(sequence)
    nonreceived_packets = await connection.packet_queue.get_nonverified_packets()

    await normal_operation(connection, start_packets=nonreceived_packets)


async def handshake_protocol_version(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    LOGGER.info("Handshaking protocol version.")

    writer.write(PROTOCOL_VERSION.to_bytes(1, "big"))
    await writer.drain()

    reply_bytes = await reader.readexactly(1)

    if int.from_bytes(reply_bytes, "big") != PROTOCOL_VERSION:
        writer.close()
        await writer.wait_closed()
        raise GenericException("Version mismatch.")

    LOGGER.debug("Version handshake successful.")


async def setup_new_connection(
    volatile_reader: asyncio.StreamReader, volatile_writer: asyncio.StreamWriter
):
    LOGGER.info("Setting up new connection.")

    port_bytes = await volatile_reader.readexactly(2)
    LOGGER.debug("Port bytes: %s", port_bytes)

    port = int.from_bytes(port_bytes, "big")
    LOGGER.debug("Port: %d", port)

    stable_reader, stable_writer = await asyncio.open_connection("localhost", port)

    uuid = uuid4()
    LOGGER.debug("Assigning UUID: %s", uuid)

    connection = Connection(
        volatile_reader,
        volatile_writer,
        stable_reader,
        stable_writer,
        uuid=uuid,
        max_buffer_size=Constants.MAX_BUFFER_SIZE,
    )

    volatile_writer.write(uuid.bytes)
    await volatile_writer.drain()

    uuid_ack_bytes = await volatile_reader.readexactly(16)
    uuid_ack = UUID(bytes=uuid_ack_bytes)
    LOGGER.debug("UUID ACK: %s", uuid_ack)

    if uuid_ack != uuid:
        await connection.close(True)
        raise GenericException("Handshake error.")

    LOGGER.info("Connection established.")

    async with CONNECTION_DICT_SYNC:
        if len(alive_connections) + len(dead_connections) >= args.max_connections:
            await connection.close(True)
            raise GenericException("Connection limit reached.")

        task = asyncio.create_task(normal_operation(connection))
        alive_connections[uuid] = connection
        alive_tasks.add(task, uuid)
        CONNECTION_DICT_SYNC.notify()


async def setup_old_connection(
    volatile_reader: asyncio.StreamReader, volatile_writer: asyncio.StreamWriter
):
    LOGGER.debug("Setting up old connection.")
    uuid_bytes = await volatile_reader.readexactly(16)
    uuid = UUID(bytes=uuid_bytes)

    LOGGER.debug("Received UUID: %s", uuid)

    async with CONNECTION_DICT_SYNC:
        if uuid in alive_connections:
            LOGGER.debug("Connection found in alive tasks.")
            task = alive_tasks.pop1(uuid)
            LOGGER.debug("Cancelling old task for UUID: %s", uuid)
            task.cancel()
            LOGGER.debug("Checking that task is done.")
            if not task.done():
                raise RuntimeError("Task not done after cancellation.")
            connection = alive_connections.pop(uuid)

            LOGGER.debug("Closing old connection writer.")
            connection.volatile_writer.close()
            await connection.volatile_writer.wait_closed()

        elif uuid in dead_connections:
            LOGGER.debug("Connection found in dead connections")
            connection, _ = dead_connections.pop(uuid)

        LOGGER.debug("Setting new connection parameters.")
        connection.volatile_reader = volatile_reader
        connection.volatile_writer = volatile_writer

        alive_connections[uuid] = connection
        alive_tasks.add(
            asyncio.create_task(normal_operation(connection)), uuid
        )
        CONNECTION_DICT_SYNC.notify()


async def setup_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    LOGGER.info("Handshaking connection ID.")

    connection_code_bytes = await reader.readexactly(1)
    LOGGER.debug(f"Connection code bytes: {connection_code_bytes}")

    connection_code = int.from_bytes(connection_code_bytes, "big")
    LOGGER.debug(f"Connection code: {connection_code}")

    if connection_code == Constants.NEW_CONNECTION:
        await setup_new_connection(reader, writer)
    elif connection_code == Constants.OLD_CONNECTION:
        await setup_old_connection(reader, writer)


async def new_connection_callback(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    async with NEW_CONNECTION_SEM:
        LOGGER.info("New connection received.")

        try:
            await handshake_protocol_version(reader, writer)
            await setup_connection(reader, writer)
        except GenericException as e:
            LOGGER.error(e.message)


async def main(args: Args):
    await asyncio.start_server(new_connection_callback, "localhost", args.port)

    while True:
        async with CONNECTION_DICT_SYNC:
            notify_task = asyncio.create_task(CONNECTION_DICT_SYNC.wait())
            done_tasks, _ = await asyncio.wait(
                itertools.chain([notify_task], alive_tasks.keys0()),
                return_when=asyncio.FIRST_COMPLETED,
            )

            if notify_task not in done_tasks:
                notify_task.cancel()
                try:
                    await notify_task
                except asyncio.CancelledError:
                    pass

            for task in done_tasks:
                if task.cancelled():
                    continue

                await task

                if task is notify_task:
                    continue

                uuid = alive_tasks.pop0(task)
                connection = alive_connections.pop(uuid)

                if connection.stable_writer.is_closing():
                    LOGGER.debug("Connection %s closed.", uuid)
                    await connection.close(True)
                    continue

                else:
                    LOGGER.debug("Connection %s died.", uuid)
                    dead_connections[uuid] = (connection, datetime.datetime.now())


if __name__ == "__main__":
    args = Args(underscores_to_dashes=True).parse_args()
    asyncio.run(main(args))
