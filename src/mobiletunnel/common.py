import asyncio
import collections
import dataclasses
from uuid import UUID
from typing import Generic, TypeVar
from .logging import get_logger


LOGGER = get_logger(__name__)

S = TypeVar("S")
T = TypeVar("T")

class KeyKeyStore(Generic[S, T]):
    def __init__(self):
        self._a: dict[S, T] = {}
        self._b: dict[T, S] = {}

    def add(self, key_a: S, key_b: T):
        if key_a in self._a or key_b in self._b:
            raise KeyError("Key already exists in the store.")
        self._a[key_a] = key_b
        self._b[key_b] = key_a

    def get0(self, key_a: S) -> T:
        if key_a not in self._a:
            raise KeyError("Key not found in the store.")
        return self._a[key_a]
    
    def get1(self, key_b: T) -> S:
        if key_b not in self._b:
            raise KeyError("Key not found in the store.")
        return self._b[key_b]

    def pop0(self, key_a: S) -> T:
        if key_a not in self._a:
            raise KeyError("Key not found in the store.")
        key_b = self._a.pop(key_a)
        self._b.pop(key_b)
        return key_b

    def pop1(self, key_b: T) -> S:
        if key_b not in self._b:
            raise KeyError("Key not found in the store.")
        key_a = self._b.pop(key_b)
        self._a.pop(key_a)
        return key_a

    def keys0(self) -> set[S]:
        return self._a.keys()

    def __len__(self) -> int:
        return len(self._a)

class GenericException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
        self._msg = msg

    @property
    def message(self) -> str:
        return self._msg


class Constants:

    NEW_CONNECTION = 0x00
    OLD_CONNECTION = 0x01

    CONNECTION_ERROR = 0xFF

    MAX_PACKET_SIZE = 1023
    MAX_SEQUENCE_LENGTH = 64

    MAX_BUFFER_SIZE = 2097088


class PacketQueue:

    def __init__(self, max_buffer_size: int):
        self._buffer_size = 0
        self._max_buffer_size = max_buffer_size
        self._sequence_counter = 0
        self._verified_sequence_counter = 0
        self._queue = collections.deque[bytes]()
        self._queue_lock = asyncio.Condition()

    @property
    def _can_increment_counter(self) -> bool:
        return (
            self._sequence_counter + 1
        ) % Constants.MAX_SEQUENCE_LENGTH != self._verified_sequence_counter

    @property
    def _remaining_buffer_size(self) -> int:
        return self._max_buffer_size - self._buffer_size

    async def buffer_availability(self) -> int:
        """Returns the available buffer size. This function blocks until a new packet
        can be added to the queue.

        Returns
        -------
        int
            The available buffer size, in bytes.
        """

        async with self._queue_lock:
            await self._queue_lock.wait_for(
                lambda: self._remaining_buffer_size > 0 and self._can_increment_counter
            )

            return self._remaining_buffer_size

    async def add_packet(self, packet: bytes):
        """Adds a packet to the queue.

        Parameters
        ----------
        packet : bytes
            The packet to add.
        """

        async with self._queue_lock:
            assert self._remaining_buffer_size >= len(packet)
            assert self._can_increment_counter

            self._queue.append(packet)
            self._buffer_size += len(packet)
            self._sequence_counter = (
                self._sequence_counter + 1
            ) % Constants.MAX_SEQUENCE_LENGTH

    async def step_verified_counter(self, counter: int):
        """Increments the verified sequence counter.

        Parameters
        ----------
        counter : int
            The counter to increment to.
        """

        if counter < 0 or counter >= Constants.MAX_SEQUENCE_LENGTH:
            raise ValueError(
                "Counter must be between 0 (inclusive) and Constants.MAX_SEQUENCE_LENGTH (exclusive)."
            )

        async with self._queue_lock:
            if counter < self._verified_sequence_counter:
                self._verified_sequence_counter -= Constants.MAX_SEQUENCE_LENGTH
            steps = counter - self._verified_sequence_counter
            self._verified_sequence_counter = counter

            for _ in range(steps):
                self._buffer_size -= len(self._queue[0])
                self._queue.popleft()

            self._queue_lock.notify_all()

    async def get_nonverified_packets(self) -> list[bytes]:
        """Returns all packets that have not been verified yet.

        Returns
        -------
        list[bytes]
            The packets that have not been verified yet.
        """

        async with self._queue_lock:
            return list(self._queue)


class Connection:

    def __init__(
        self,
        volatile_reader: asyncio.StreamReader,
        volatile_writer: asyncio.StreamWriter,
        stable_reader: asyncio.StreamReader,
        stable_writer: asyncio.StreamWriter,
        *,
        uuid: UUID,
        max_buffer_size: int
    ):
        self.volatile_reader = volatile_reader
        self.volatile_writer = volatile_writer
        self.stable_reader = stable_reader
        self.stable_writer = stable_writer
        self.uuid = uuid

        self.volatile_writer_lock = asyncio.Lock()

        self.packet_queue = PacketQueue(max_buffer_size)
        self._received_counter = 0
        self._last_communicated_counter = 0

        self.tasks = NormalOperationTasks()

    @property
    def counter(self) -> int:
        assert 0 <= self._received_counter < Constants.MAX_SEQUENCE_LENGTH
        return self._received_counter

    async def close(self, notify_other_side: bool):

        if notify_other_side:
            async with self.volatile_writer_lock:
                if self.volatile_writer.is_closing():
                    LOGGER.warning(
                        "Cannot notify other side as volatile writer is closing."
                    )
                else:
                    self.volatile_writer.write(0x40.to_bytes(1, "big"))
                    await self.volatile_writer.drain()

        if not self.volatile_writer.is_closing():
            self.volatile_writer.close()
            await self.volatile_writer.wait_closed()

        if not self.stable_writer.is_closing():
            self.stable_writer.close()
            await self.stable_writer.wait_closed()

    async def send_packet(self, packet: bytes, *, is_resend: bool = False):
        assert len(packet) <= Constants.MAX_PACKET_SIZE
        if not is_resend:
            await self.packet_queue.add_packet(packet)

        header = 0x8000 | len(packet)
        async with self.volatile_writer_lock:
            self.volatile_writer.write(header.to_bytes(2, "big"))
            self.volatile_writer.write(packet)
            await self.volatile_writer.drain()

    async def receive_packet(self) -> bytes:

        state = 0
        bytes_to_read = 1

        while True:
            try:
                data = await self.volatile_reader.readexactly(bytes_to_read)
            except asyncio.IncompleteReadError as e:
                return b""

            if data == b"":
                return b""

            if state == 0:

                if data[0] & 0xC0 == 0:
                    verified_counter = data[0] & 0x3F
                    await self.packet_queue.step_verified_counter(verified_counter)
                    assert state == 0
                    assert bytes_to_read == 1

                elif data[0] & 0xC0 == 0x80:
                    state = data[0]
                    bytes_to_read = 1

                else:
                    assert data[0] & 0xC0 == 0x40
                    await self.close(False)
                    return b""

            elif state & 0x8000 == 0:
                state = (state << 8) | data[0]
                bytes_to_read = state & 0x7FFF

            else:
                assert state & 0x8000 == 0x8000

                self._received_counter += 1
                if self._received_counter - self._last_communicated_counter >= 10:
                    self._received_counter %= Constants.MAX_SEQUENCE_LENGTH
                    self._last_communicated_counter = self._received_counter
                    async with self.volatile_writer_lock:
                        self.volatile_writer.write(
                            self._received_counter.to_bytes(1, "big")
                        )
                        await self.volatile_writer.drain()

                return data


@dataclasses.dataclass(slots=True)
class NormalOperationTasks:
    buffer_availability: asyncio.Task[int] | None = None
    stable_read: asyncio.Task[bytes] | None = None
    stable_write: asyncio.Task[None] | None = None
    volatile_read: asyncio.Task[bytes] | None = None
    volatile_write: asyncio.Task[None] | None = None

    async def wait(self):
        tasks: list[asyncio.Task] = []

        if self.buffer_availability is not None:
            tasks.append(self.buffer_availability)
        if self.stable_read is not None:
            tasks.append(self.stable_read)
        if self.stable_write is not None:
            tasks.append(self.stable_write)
        if self.volatile_read is not None:
            tasks.append(self.volatile_read)
        if self.volatile_write is not None:
            tasks.append(self.volatile_write)

        done_tasks, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        return done_tasks


async def _normal_operation(
    connection: Connection, *, start_packets: list[bytes] | None = None
):
    if start_packets is None:
        start_packets = []

    LOGGER.info("Starting normal operation for uuid %s.", connection.uuid)

    # These are packets that should be resent before resuming normal operation.
    initial_packets = collections.deque[bytes](start_packets)

    # If we did not manage to stop the stable read task in time, we might end up
    # with a single packet that we need to send after the initial packets are exhausted.
    packet_to_send_after_initial_packets_exhausted: bytes | None = None

    tasks = connection.tasks

    if len(initial_packets) > 0:
        tasks.volatile_write = asyncio.create_task(
            connection.send_packet(initial_packets.popleft(), is_resend=True)
        )
    elif tasks.stable_read is None:
        tasks.buffer_availability = asyncio.create_task(
            connection.packet_queue.buffer_availability()
        )

    tasks.volatile_read = asyncio.create_task(connection.receive_packet())

    while True:

        for task in await tasks.wait():

            if task is tasks.buffer_availability:
                buffer_availability = await tasks.buffer_availability
                tasks.buffer_availability = None
                assert tasks.stable_read is None
                tasks.stable_read = asyncio.create_task(
                    connection.stable_reader.read(
                        min(buffer_availability, Constants.MAX_PACKET_SIZE)
                    )
                )

            elif task is tasks.stable_read:
                packet = await tasks.stable_read

                if packet == b"":
                    # Connection closed from stable side.
                    await connection.close(True)
                    return

                tasks.stable_read = None
                # If we other packets to send, store this packet to send after
                # the initial packets are exhausted.
                if len(initial_packets) > 0:
                    assert packet_to_send_after_initial_packets_exhausted is None
                    packet_to_send_after_initial_packets_exhausted = packet
                else:
                    tasks.volatile_write = asyncio.create_task(
                        connection.send_packet(packet)
                    )

            elif task is tasks.stable_write:
                await tasks.stable_write
                tasks.stable_write = None
                tasks.volatile_read = asyncio.create_task(connection.receive_packet())

            elif task is tasks.volatile_read:
                data = await tasks.volatile_read
                tasks.volatile_read = None

                if data == b"":
                    # Connection closed or lost from volatile side.
                    return

                connection.stable_writer.write(data)
                tasks.stable_write = asyncio.create_task(
                    connection.stable_writer.drain()
                )

            else:
                assert task is tasks.volatile_write
                await tasks.volatile_write
                tasks.volatile_write = None

                if len(initial_packets) > 0:
                    tasks.volatile_write = asyncio.create_task(
                        connection.send_packet(
                            initial_packets.popleft(), is_resend=True
                        )
                    )
                elif packet_to_send_after_initial_packets_exhausted is not None:
                    tasks.volatile_write = asyncio.create_task(
                        connection.send_packet(
                            packet_to_send_after_initial_packets_exhausted,
                        )
                    )
                    packet_to_send_after_initial_packets_exhausted = None
                else:
                    tasks.buffer_availability = asyncio.create_task(
                        connection.packet_queue.buffer_availability()
                    )


async def normal_operation(
    connection: Connection, *, start_packets: list[bytes] | None = None
):
    try:
        await _normal_operation(connection, start_packets=start_packets)
    except Exception as e:
        LOGGER.exception("An error occurred during normal operation.")
