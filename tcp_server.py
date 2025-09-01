# server.txt
# -*- coding: utf-8 -*-
import asyncio
import logging
import random
import datetime

class ClientHandler:
    """
    Handler for a single TCP client.
    Responsible for receiving messages, processing them, and sending responses.
    """
    def __init__(self, reader, writer, client_id, server):
        self.reader = reader
        self.writer = writer
        self.client_id = client_id
        self.server = server
        self.tasks = []  # list of active client tasks

    async def send_message(self, message: str):
        """Sends a message to the client."""
        try:
            self.writer.write(message.encode('ascii'))
            await self.writer.drain()
        except Exception as e:
            self.server.error_logger.error(f"Send error to client {self.client_id}: {e}")

    async def send_keepalive(self):
        """Periodically sends keepalive messages."""
        self.server.global_response_counter += 1
        try:
            keepalive_message = f"keepalive [{self.server.global_response_counter}]\n"
            self.writer.write(keepalive_message.encode('ascii'))
            await self.writer.drain()
        except Exception as e:
            self.server.global_response_counter -= 1
            self.server.error_logger.error(f"Keepalive error for client {self.client_id}: {e}")

    async def handle(self):
        """Main loop for processing client messages."""
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break

                message = data.decode('ascii').strip()
                if message:
                    dt_recv = datetime.datetime.now()
                    date_str = dt_recv.strftime('%Y-%m-%d')
                    time_recv = dt_recv.strftime('%H:%M:%S.%f')[:-3]

                    # simulate the probability of ignoring a message
                    if random.randint(1, 10) > 1:
                        try:
                            request_number = int(message.split()[0][1:-1])  # extract number from [N]
                            task = asyncio.create_task(
                                self.process_message(request_number, date_str, time_recv, message)
                            )
                            self.tasks.append(task)
                        except Exception as e:
                            self.server.error_logger.error(f"Message parse error from client {self.client_id}: {e}")
                    else:
                        self.log_event(date_str, time_recv, message, "(ignored)", "(ignored)")
        except asyncio.CancelledError:
            # proper termination on cancellation
            pass
        except Exception as e:
            self.server.error_logger.error(f"Client {self.client_id} error: {e}")
        finally:
            # cancel and wait for all tasks to complete
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)

            self.writer.close()
            await self.writer.wait_closed()
            self.server.clients.pop(self.client_id, None)

    async def process_message(self, request_number, date_str, time_recv, text_query):
        """Simulates processing of a client message."""
        try:
            delay = random.uniform(0.1, 1.0)
            await asyncio.sleep(delay)
            self.server.global_response_counter += 1

            response = f"[{self.server.global_response_counter}/{request_number}] PONG ({self.client_id})\n"
            dt_send = datetime.datetime.now()
            time_send = dt_send.strftime('%H:%M:%S.%f')[:-3]

            await self.send_message(response)
            self.log_event(date_str, time_recv, text_query, time_send, response.strip())
        except Exception as e:
            self.server.error_logger.error(f"Processing error for client {self.client_id}: {e}")

    def log_event(self, date_str, time_recv, text_query, time_send, response):
        """Unified method for logging client events."""
        log_line = f"{date_str};{time_recv};{text_query};{time_send};{response}\n"
        self.server.logger.info(log_line)


class TCPServer:
    """Asynchronous TCP server."""
    def __init__(self):
        self.clients = {}
        self.client_counter = 0
        self.global_response_counter = 0
        self.log_file = 'server_log.txt'
        self.error_log_file = 'server_error.log'
        self.setup_logging()

    def setup_logging(self):
        """Sets up logging: separate for events and errors."""
        # event log
        self.logger = logging.getLogger('server_logger')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # error log
        self.error_logger = logging.getLogger('server_error_logger')
        self.error_logger.setLevel(logging.ERROR)
        error_handler = logging.FileHandler(self.error_log_file, encoding='utf-8')
        error_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        self.error_logger.addHandler(error_handler)

    async def handle_client(self, reader, writer):
        """Creates a handler for a new client."""
        self.client_counter += 1
        client_id = self.client_counter
        client_handler = ClientHandler(reader, writer, client_id, self)
        self.clients[client_id] = client_handler
        await client_handler.handle()

    async def send_keepalive(self):
        """Background task: sends keepalive messages to all clients every 5 seconds."""
        while True:
            await asyncio.sleep(5)
            for client_handler in list(self.clients.values()):
                await client_handler.send_keepalive()

    async def run(self, host='127.0.0.1', port=8888):
        """Starts the TCP server."""
        server = await asyncio.start_server(self.handle_client, host, port)
        async with server:
            try:
                await asyncio.gather(server.serve_forever(), self.send_keepalive())
            except asyncio.CancelledError:
                pass
            finally:
                for handler in self.logger.handlers:
                    if isinstance(handler, logging.FileHandler):
                        handler.flush()
                for handler in self.error_logger.handlers:
                    if isinstance(handler, logging.FileHandler):
                        handler.flush()


async def main():
    server = TCPServer()
    await server.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped by user")
    except Exception as e:
        logging.basicConfig(filename="server_fatal.log", level=logging.ERROR, encoding='utf-8')
        logging.error(f"Fatal server error: {e}")
