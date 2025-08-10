# server.txt
# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import random
import datetime

class ClientHandler:
    def __init__(self, reader, writer, client_id, server):
        self.reader = reader
        self.writer = writer
        self.client_id = client_id
        self.server = server

    async def send_message(self, message):
        try:
            self.writer.write(message.encode('ascii'))
            await self.writer.drain()
        except Exception as e:
            self.server.logger.error(f"Error sending message to client {self.client_id}: {e}")

    async def send_keepalive(self):
        self.server.global_response_counter += 1
        try:
            keepalive_message = f"keepalive [{self.server.global_response_counter}]\n"
            self.writer.write(keepalive_message.encode('ascii'))
            await self.writer.drain()
        except Exception as e:
            self.server.global_response_counter -= 1
            self.server.logger.error(f"Error sending message to client {self.client_id}: {e}")

    async def handle(self):
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
                    text_query = message
                    if random.randint(1, 10) > 1:
                        request_number = int(message.split()[0][1:-1])  # Extract number from [N]
                        asyncio.create_task(self.process_message(request_number, date_str, time_recv, text_query))
                    else:
                        log_line = f"{date_str};{time_recv};{text_query};(проигнорировано);(проигнорировано)\n"
                        self.server.logger.info(log_line)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.server.logger.error(f"Error with client {self.client_id}: {e}")
        finally:
            self.writer.close()
            await self.writer.wait_closed()
            if self.client_id in self.server.clients:
                del self.server.clients[self.client_id]

    async def process_message(self, request_number, date_str, time_recv, text_query):
        try:
            delay = random.uniform(0.1, 1.0)
            await asyncio.sleep(delay)
            self.server.global_response_counter += 1
            response = f"[{self.server.global_response_counter}/{request_number}] PONG ({self.client_id})\n"
            dt_send = datetime.datetime.now()
            time_send = dt_send.strftime('%H:%M:%S.%f')[:-3]
            await self.send_message(response)
            log_line = f"{date_str};{time_recv};{text_query};{time_send};{response.strip()}\n"
            self.server.logger.info(log_line)
        except Exception as e:
            self.server.logger.error(f"Error processing message for client {self.client_id}: {e}")

class TCPServer:
    def __init__(self):
        self.clients = {}
        self.client_counter = 0
        self.global_response_counter = 0
        self.log_file = 'server_log.txt'
        self.setup_logging()

    def setup_logging(self):
        self.logger = logging.getLogger('server_logger')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    async def handle_client(self, reader, writer):
        self.client_counter += 1
        client_id = self.client_counter
        client_handler = ClientHandler(reader, writer, client_id, self)
        self.clients[client_id] = client_handler
        await client_handler.handle()
        if client_id in self.clients:
            del self.clients[client_id]

    async def send_keepalive(self):
        while True:
            await asyncio.sleep(5)
            for client_handler in list(self.clients.values()):
                await client_handler.send_keepalive()

    async def run(self, host='127.0.0.1', port=8888):
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

async def main():
    server = TCPServer()
    await server.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        pass