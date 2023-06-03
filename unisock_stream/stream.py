import asyncio
import os
import socket
from pathlib import Path
from typing import Any, Callable, Coroutine

from unisock_stream import vars

DEFAULT_TIMEOUT = (10.0, 3.0)


class AsyncSocketStreamReader:
    def __init__(
        self,
        callback: Callable[[bytes], Any | Coroutine[Any, Any, Any]],
        timeout: tuple[float, float] = vars.DEFAULT_TIMEOUT,
    ):
        self.callback: Callable[[bytes], Any | Coroutine[Any, Any, Any]] = callback
        self.timeout: tuple[float, float] = timeout

        self.from_pid: int = vars.PID
        self.on_windows: bool = os.name == "nt"

        self.file: Path = Path(
            "./sock_stream.{}.{}.sock".format(self.from_pid, vars.counter)
        ).resolve()

        if self.on_windows:
            prefix: str = "\\\\.\\pipe\\"
        else:
            prefix: str = "unix:"

        self.url: str = prefix + str(self.file)

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        self._sock.bind(str(self.file))

        self._sock.listen(1)

        self._conn: socket.socket | None = None

        self.__stopped: bool = False

        vars.counter += 1

    @property
    def stopped(self):
        return self.__stopped

    @stopped.setter
    def stopped(self, value: bool):
        self.__stopped = value

        if self.stopped:
            functions: list[tuple[Callable, list[Any]]] = [
                (self._sock.close, []),
                (os.remove, [self.file]),
            ]

            if self._conn:
                functions.insert(0, (self._conn.close, []))

            for func, args in functions:
                try:
                    func(*args)
                except:
                    pass

    async def work(self):
        loop = asyncio.get_running_loop()

        try:
            conn, address = await asyncio.wait_for(
                loop.sock_accept(self._sock), self.timeout[0]
            )
        except asyncio.TimeoutError:
            self.stoped = True
            return

        self._conn = conn

        while True:
            if self.stopped:
                return

            try:
                data: bytes = await asyncio.wait_for(
                    loop.sock_recv(conn, 4096), self.timeout[1]
                )
            except asyncio.TimeoutError:
                self.stopped = True
                return

            result = await asyncio.to_thread(self.callback, data)

            if isinstance(result, Coroutine):
                result = await result

            if result:
                self.stopped = True
                return


class AsyncSocketStreamWriter:
    def __init__(
        self,
        callback: Callable[[], bytes | Coroutine[Any, Any, bytes | None] | None],
        timeout: tuple[float, float] = vars.DEFAULT_TIMEOUT,
    ):
        self.callback: Callable[
            [], bytes | Coroutine[Any, Any, bytes | None] | None
        ] = callback
        self.timeout: tuple[float, float] = timeout

        self.from_pid: int = vars.PID
        self.on_windows: bool = os.name == "nt"

        self.file: Path = Path(
            "./sock_stream.{}.{}.sock".format(self.from_pid, vars.counter)
        ).resolve()

        if self.on_windows:
            prefix: str = "\\\\.\\pipe\\"
        else:
            prefix: str = "unix:"

        self.url: str = prefix + str(self.file)

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        self._sock.bind(str(self.file))

        self._sock.listen(1)

        self.__stopped: bool = False

        vars.counter += 1

    @property
    def stopped(self):
        return self.__stopped

    @stopped.setter
    def stopped(self, value: bool):
        self.__stopped = value

        if self.stopped:
            functions: list[tuple[Callable, list[Any]]] = [
                (self._sock.close, []),
                (os.remove, [self.file]),
            ]

            if self._conn:
                functions.insert(0, (self._conn.close, []))

            for func, args in functions:
                try:
                    func(*args)
                except:
                    pass

    async def work(self):
        loop = asyncio.get_running_loop()

        try:
            conn, address = await asyncio.wait_for(
                loop.sock_accept(self._sock), self.timeout[0]
            )
        except asyncio.TimeoutError:
            self.stopped = True
            return

        self._conn = conn

        while True:
            if self.stopped:
                return

            data = await asyncio.to_thread(self.callback)

            if isinstance(data, Coroutine):
                data = await data

            if data == None:
                self.stopped = True
                return

            try:
                await asyncio.wait_for(
                    loop.sock_sendall(self._conn, data), self.timeout[1]
                )
            except asyncio.TimeoutError:
                self.stopped = True
                return


class SocketStreamReader:
    def __init__(
        self,
        callback: Callable[[bytes], Any],
        timeout: tuple[float, float] = vars.DEFAULT_TIMEOUT,
    ):
        self.callback: Callable[[bytes], Any] = callback
        self.timeout: tuple[float, float] = timeout

        self.from_pid: int = vars.PID
        self.on_windows: bool = os.name == "nt"

        self.file: Path = Path(
            "./sock_stream.{}.{}.sock".format(self.from_pid, vars.counter)
        ).resolve()

        if self.on_windows:
            prefix: str = "\\\\.\\pipe\\"
        else:
            prefix: str = "unix:"

        self.url: str = prefix + str(self.file)

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        self._sock.bind(str(self.file))

        self._sock.listen(1)

        self._conn: socket.socket | None = None

        self.__stopped: bool = False

        vars.counter += 1

    @property
    def stopped(self):
        return self.__stopped

    @stopped.setter
    def stopped(self, value: bool):
        self.__stopped = value

        if self.stopped:
            functions: list[tuple[Callable, list[Any]]] = [
                (self._sock.close, []),
                (os.remove, [self.file]),
            ]

            if self._conn:
                functions.insert(0, (self._conn.close, []))

            for func, args in functions:
                try:
                    func(*args)
                except:
                    pass

    def work(self):
        self._sock.settimeout(self.timeout[0])

        try:
            conn, address = self._sock.accept()
        except TimeoutError:
            self.stoped = True
            return

        self._conn = conn

        self._conn.settimeout(self.timeout[1])

        while True:
            if self.stoped:
                return

            try:
                data: bytes = self._conn.recv(4096)
            except TimeoutError:
                self.stopped = True
                return

            raised = self.callback(data)

            if raised:
                self.stopped = True
                return


class SocketStreamWriter:
    def __init__(
        self,
        callback: Callable[[], bytes | None],
        timeout: float = vars.DEFAULT_TIMEOUT[0],
    ):
        self.callback: Callable[[], bytes | None] = callback
        self.timeout: float = timeout

        self.from_pid: int = vars.PID
        self.on_windows: bool = os.name == "nt"

        self.file: Path = Path(
            "./sock_stream.{}.{}.sock".format(self.from_pid, vars.counter)
        ).resolve()

        if self.on_windows:
            prefix: str = "\\\\.\\pipe\\"
        else:
            prefix: str = "unix:"

        self.url: str = prefix + str(self.file)

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        self._sock.bind(str(self.file))

        self._sock.listen(1)

        self.__stopped: bool = False

        vars.counter += 1

    @property
    def stopped(self):
        return self.__stopped

    @stopped.setter
    def stopped(self, value: bool):
        self.__stopped = value

        if self.stopped:
            functions: list[tuple[Callable, list[Any]]] = [
                (self._sock.close, []),
                (os.remove, [self.file]),
            ]

            if self._conn:
                functions.insert(0, (self._conn.close, []))

            for func, args in functions:
                try:
                    func(*args)
                except:
                    pass

    def work(self):
        self._sock.settimeout(self.timeout)

        try:
            conn, address = self._sock.accept()
        except TimeoutError:
            self.stoped = True
            return

        self._conn = conn

        while True:
            if self.stopped:
                return

            data = self.callback()

            if data == None:
                self.stopped = True
                return

            self._conn.sendall(data)
