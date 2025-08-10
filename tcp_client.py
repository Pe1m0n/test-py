# client.txt
# -*- coding: utf-8 -*-
import asyncio
from collections import defaultdict
import random
import datetime
import logging

class TCPClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.running = True
        self.message_queue = asyncio.Queue()
        self.responses = defaultdict(list)
        self.final_responses = {}
        self.pending_requests = []
        self.send_times = {}
        self.response_events = {}
        self.request_data = {}
        self.log_file = 'client_log.txt'
        self.setup_logging()

    def setup_logging(self):
        self.logger = logging.getLogger('client_logger')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        except Exception as e:
            raise

    async def send_ping(self, request_number):
        try:
            message = f"[{request_number}] PING\n"
            dt_send = datetime.datetime.now()
            date_str = dt_send.strftime('%Y-%m-%d')
            time_send = dt_send.strftime('%H:%M:%S.%f')[:-3]
            text_query = message.strip()
            self.request_data[request_number] = {'date': date_str, 'time_send': time_send, 'text_query': text_query}
            self.writer.write(message.encode('ascii'))
            await self.writer.drain()
            self.pending_requests.append(request_number)
            self.send_times[request_number] = asyncio.get_event_loop().time()
            self.response_events[request_number] = asyncio.Event()
        except Exception as e:
            raise

    async def listen_for_messages(self):
        while self.running:
            try:
                data = await self.reader.readline()
                if not data:
                    self.running = False
                    return
                message = data.decode('ascii').strip()
                await self.message_queue.put(message)
            except Exception as e:
                self.running = False
                return

    async def process_messages(self):
        while self.running:
            message = await self.message_queue.get()
            dt_recv = datetime.datetime.now()
            date_str = dt_recv.strftime('%Y-%m-%d')
            time_recv = dt_recv.strftime('%H:%M:%S.%f')[:-3]
            if "keepalive" in message.lower():
                log_line = f"{date_str};;;{time_recv};{message}\n"
                self.logger.info(log_line)
            else:
                request_number = int(message.split('/')[1].split(']')[0]) if '/' in message else None
                if request_number is not None:
                    self.responses[request_number].append(message)
                    if request_number in self.response_events:
                        self.response_events[request_number].set()
            self.message_queue.task_done()

    async def monitor_responses(self, timeout=5.0):
        while self.running:
            to_remove = []
            for request_number in list(self.pending_requests):
                current_time = asyncio.get_event_loop().time()
                elapsed = current_time - self.send_times.get(request_number, 0)
                if self.responses[request_number]:
                    response = self.responses[request_number].pop(0)
                    dt_recv = datetime.datetime.now()
                    time_recv = dt_recv.strftime('%H:%M:%S.%f')[:-3]
                    data = self.request_data.get(request_number, {})
                    date_str = data.get('date', '')
                    time_send = data.get('time_send', '')
                    text_query = data.get('text_query', '')
                    log_line = f"{date_str};{time_send};{text_query};{time_recv};{response}\n"
                    self.logger.info(log_line)
                    to_remove.append(request_number)
                elif elapsed > timeout:
                    dt_timeout = datetime.datetime.now()
                    time_timeout = dt_timeout.strftime('%H:%M:%S.%f')[:-3]
                    data = self.request_data.get(request_number, {})
                    date_str = data.get('date', '')
                    time_send = data.get('time_send', '')
                    text_query = data.get('text_query', '')
                    log_line = f"{date_str};{time_send};{text_query};{time_timeout};(таймаут)\n"
                    self.logger.info(log_line)
                    to_remove.append(request_number)
            for req in to_remove:
                if req in self.pending_requests:
                    self.pending_requests.remove(req)
                if req in self.responses:
                    del self.responses[req]
                if req in self.response_events:
                    del self.response_events[req]
                if req in self.send_times:
                    del self.send_times[req]
                if req in self.request_data:
                    del self.request_data[req]
            await asyncio.sleep(0)

    async def close(self):
        self.running = False
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        for handler in self.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()

async def main():
    client = TCPClient('127.0.0.1', 8888)
    i = 0
    try:
        await client.connect()
        listen_task = asyncio.create_task(client.listen_for_messages())
        process_task = asyncio.create_task(client.process_messages())
        monitor_task = asyncio.create_task(client.monitor_responses())
        
        while True:
            try:
                await client.send_ping(i)
                i += 1
                delay = random.uniform(0.3, 3.0)
                await asyncio.sleep(delay)
            except (ConnectionError, Exception) as e:
                await client.close()
                await asyncio.sleep(1)
                await client.connect()
                listen_task = asyncio.create_task(client.listen_for_messages())
                process_task = asyncio.create_task(client.process_messages())
                monitor_task = asyncio.create_task(client.monitor_responses())
        while client.running and any(task for task in [listen_task, process_task, monitor_task] if not task.done()):
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        pass
    finally:
        await client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        pass