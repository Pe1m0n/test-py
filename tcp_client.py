# client.txt
# -*- coding: utf-8 -*-
import asyncio
from collections import defaultdict
import random
import datetime
import logging
from typing import Dict, List, Optional


class TCPClient:
    """
    Asynchronous TCP client.

    Responsible for connecting to the server, sending PING requests,
    receiving and parsing messages, logging results and errors.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

        # network objects
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

        # flags and queues
        self.running: bool = True
        self.message_queue: asyncio.Queue[str] = asyncio.Queue()

        # request/response states
        self.responses: Dict[int, List[str]] = defaultdict(list)
        self.pending_requests: List[int] = []
        self.send_times: Dict[int, float] = {}
        self.request_data: Dict[int, Dict[str, str]] = {}

        # tasks that need to be properly closed
        self.tasks: List[asyncio.Task] = []

        # log files
        self.log_file = 'client_log.txt'         # events/exchange log
        self.error_log_file = 'client_error.log' # errors
        self.setup_logging()

    # --------------------------- LOGGING ---------------------------------
    def setup_logging(self) -> None:
        """Set up two separate loggers: for events and errors."""
        # events
        self.logger = logging.getLogger('client_logger')
        self.logger.setLevel(logging.INFO)
        fmt_events = logging.Formatter('%(message)s')
        fh_events = logging.FileHandler(self.log_file)
        fh_events.setFormatter(fmt_events)
        self.logger.addHandler(fh_events)

        # errors
        self.error_logger = logging.getLogger('client_error_logger')
        self.error_logger.setLevel(logging.ERROR)
        fmt_errors = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        fh_errors = logging.FileHandler(self.error_log_file)
        fh_errors.setFormatter(fmt_errors)
        self.error_logger.addHandler(fh_errors)

    # --------------------------- NETWORK OPERATIONS ----------------------------
    async def connect(self) -> None:
        """Opens a connection to the server."""
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        except Exception as e:
            # important to escalate — let the upper level decide what to do
            self.error_logger.error(f"Connect error: {e}")
            raise

    async def send_ping(self, request_number: int) -> None:
        """Sends a single PING request in the format: '[N] PING\n'."""
        try:
            message = f"[{request_number}] PING\n"
            dt_send = datetime.datetime.now()
            date_str = dt_send.strftime('%Y-%m-%d')
            time_send = dt_send.strftime('%H:%M:%S.%f')[:-3]

            # save the original request data for the final log entry
            self.request_data[request_number] = {
                'date': date_str,
                'time_send': time_send,
                'text_query': message.strip(),
            }

            assert self.writer is not None, "Writer is not initialized"
            self.writer.write(message.encode('ascii'))
            await self.writer.drain()

            self.pending_requests.append(request_number)
            self.send_times[request_number] = asyncio.get_running_loop().time()
        except Exception as e:
            self.error_logger.error(f"Send PING error (req={request_number}): {e}")
            raise

    # --------------------------- MESSAGE PROCESSING -------------------------
    async def listen_for_messages(self) -> None:
        """Reads lines from the socket and puts them into the message queue."""
        while self.running:
            try:
                assert self.reader is not None, "Reader is not initialized"
                data = await self.reader.readline()
                if not data:
                    # server closed the connection
                    self.running = False
                    return
                message = data.decode('ascii').strip()
                await self.message_queue.put(message)
            except asyncio.CancelledError:
                # proper cancellation
                return
            except Exception as e:
                self.error_logger.error(f"Listen error: {e}")
                self.running = False
                return

    async def process_messages(self) -> None:
        """Parses messages from the queue: keepalive / responses to requests."""
        while self.running:
            try:
                message = await self.message_queue.get()

                dt_recv = datetime.datetime.now()
                date_str = dt_recv.strftime('%Y-%m-%d')
                time_recv = dt_recv.strftime('%H:%M:%S.%f')[:-3]

                if "keepalive" in message.lower():
                    # keepalive format does not contain request fields, so only log the receipt time
                    log_line = f"{date_str};; ;{time_recv};{message}\n"
                    self.logger.info(log_line)
                else:
                    # expected server response format: "[<global>/<request_number>] PONG (<client_id>)"
                    req_num = self._extract_request_number(message)
                    if req_num is not None:
                        self.responses[req_num].append(message)
                self.message_queue.task_done()
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.error_logger.error(f"Process message error: {e}")

    def _extract_request_number(self, message: str) -> Optional[int]:
        """Extracts request_number from the server response. Returns None on failure."""
        try:
            # example: "[123/45] PONG (7)" -> take the part after '/' and before ']'
            if '/' in message:
                right = message.split('/', 1)[1]
                return int(right.split(']', 1)[0])
        except Exception as e:
            self.error_logger.error(f"Parse response request_number error: {e}; msg='{message}'")
        return None

    # --------------------------- RESPONSE MONITORING --------------------------
    async def monitor_responses(self, timeout: float = 5.0, poll_interval: float = 0.05) -> None:
        """
        Periodically checks if a response has been received for each pending request
        or if the timeout has expired. Duplicated logic is moved to _finalize_request().
        """
        loop = asyncio.get_running_loop()
        while self.running:
            try:
                now = loop.time()
                to_finalize: List[int] = []

                for request_number in list(self.pending_requests):
                    if self.responses.get(request_number):
                        # response received — take the first one
                        response = self.responses[request_number].pop(0)
                        self._finalize_request(request_number, response, is_timeout=False)
                        to_finalize.append(request_number)
                    elif now - self.send_times.get(request_number, 0.0) > timeout:
                        # timeout
                        self._finalize_request(request_number, '(timeout)', is_timeout=True)
                        to_finalize.append(request_number)

                # clean up structures for finalized requests
                for rn in to_finalize:
                    self._cleanup_request(rn)

                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.error_logger.error(f"Monitor error: {e}")

    def _finalize_request(self, request_number: int, result: str, *, is_timeout: bool) -> None:
        """Forms a log entry for a response or timeout and writes it to the event log."""
        dt_recv = datetime.datetime.now()
        time_recv = dt_recv.strftime('%H:%M:%S.%f')[:-3]

        data = self.request_data.get(request_number, {})
        date_str = data.get('date', '')
        time_send = data.get('time_send', '')
        text_query = data.get('text_query', '')

        payload = result if not is_timeout else result  # '(timeout)' is already prepared
        log_line = f"{date_str};{time_send};{text_query};{time_recv};{payload}\n"
        self.logger.info(log_line)

    def _cleanup_request(self, request_number: int) -> None:
        """Removes all traces of a request from internal structures."""
        if request_number in self.pending_requests:
            self.pending_requests.remove(request_number)
        self.responses.pop(request_number, None)
        self.send_times.pop(request_number, None)
        self.request_data.pop(request_number, None)

    # --------------------------- LIFECYCLE -------------------------------
    def start_background_tasks(self) -> None:
        """Creates and registers background tasks for the client."""
        self.tasks = [
            asyncio.create_task(self.listen_for_messages()),
            asyncio.create_task(self.process_messages()),
            asyncio.create_task(self.monitor_responses()),
        ]

    async def close(self) -> None:
        """Stops the client and properly closes all resources and tasks."""
        self.running = False

        # cancel and wait for background tasks
        for t in self.tasks:
            if not t.done():
                t.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

        # close the connection
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                self.error_logger.error(f"Writer close error: {e}")
        self.writer = None
        self.reader = None

        # ensure loggers' file buffers are flushed
        for handler in self.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        for handler in self.error_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()


# ------------------------------- ENTRY POINT --------------------------------
async def main() -> None:
    client = TCPClient('127.0.0.1', 8888)
    request_counter = 0

    try:
        while True:
            try:
                await client.connect()
                client.running = True
                client.start_background_tasks()

                # main loop for sending requests
                while client.running:
                    await client.send_ping(request_counter)
                    request_counter += 1
                    await asyncio.sleep(random.uniform(0.3, 3.0))

            except (ConnectionError, OSError, asyncio.IncompleteReadError) as e:
                # connection lost — attempt to reconnect
                client.error_logger.error(f"Connection lost, will retry: {e}")
                await client.close()
                await asyncio.sleep(1.0)
                continue
    except asyncio.CancelledError:
        # proper cancellation
        pass
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Client stopped by user")
    except Exception as e:
        # fatal errors (if something crashes outside coroutines)
        logging.basicConfig(filename='client_fatal.log', level=logging.ERROR)
        logging.error(f"Fatal client error: {e}")
