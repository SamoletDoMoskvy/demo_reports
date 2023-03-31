from aiohttp import ClientSession
import aiofiles
from aiocsv import AsyncWriter

import asyncio
import logging
import time
import uuid
import sys
from enum import Enum
import argparse


# Define CLI arguments
arguments_parser = argparse.ArgumentParser(
    prog="demo_reports",
    epilog="Example usage: python main.py run -t <api_token> -c <int>",
)

arguments_parser.add_argument("run", help="Run program", default=True, type=bool)
arguments_parser.add_argument(
    "-t", "--token", help="API token", required=True, type=str
)
arguments_parser.add_argument(
    "-c", "--count-workers", help="Count of report workers", default=2, type=int
)

# Define Logger settings
logging.basicConfig(
    level=logging.DEBUG,
    filename="demo.log",
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger()
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(stream_handler)


class HTTPMethods(Enum):
    POST = "POST"
    PUT = "PUT"
    GET = "GET"


class APINamespaces(Enum):
    reports = "reports"


class StatusCodeDescription(Enum):
    reports_get: dict = {
        "200": "Report ready and sent in response body.",
        "202": "Report creation not yet ready",
        "400": "The request path does not match the specification.",
        "404": "A report with this id does not exist.",
        "401": "Authorization failed.",
        "429": "Limit is exceeded.",
    }
    reports_put_post: dict = {
        "201": "The request to create a report has been accepted.",
        "400": "The request path or body does not match the specification.",
        "401": "Authorization failed.",
        "409": "A report with this id already exists.",
        "429": "Limit is exceeded.",
    }


class Loader:
    _filename = "demo_output.csv"

    @classmethod
    async def write_data_into_csv_file(cls, data: dict) -> None:
        async with aiofiles.open(cls._filename, "a") as file:
            async_writer = AsyncWriter(file, delimiter=";")
            await async_writer.writerow(list(data.values()))
            logger.info(f"[Loader] Write data to file. Data: {data}")


class Extractor:
    URL: str = "https://analytics.maximum-auto.ru/vacancy-test/api/v0.1"

    def __init__(self, api_token: str, number_of_workers: int):
        self._token: str = api_token
        self._number_of_workers = number_of_workers
        self._reports_queue = asyncio.Queue()

    async def _send_request(
        self,
        api_method: HTTPMethods,
        path: str,
        payload: dict | None = None,
    ):
        async with ClientSession() as session:
            headers: dict = {
                "Authorization": f"Bearer {self._token}",
                "accept": "application/json",
                "Content-Type": "application/json",
            }
            url = f"{self.URL}/{path}" if path else self.URL
            kwargs = dict(method=api_method.value, url=url, headers=headers)
            kwargs.update(dict(json=payload)) if payload else None
            async with session.request(**kwargs) as response:
                if response.status == 200:
                    response_data = await response.json()
                else:
                    response_data = await response.text()
                return response.status, response_data

    async def _reports_worker(self, queue, worker_name: str | None = None):
        logger.info(f"Running worker #{worker_name}")
        while True:
            queue_data: dict = await queue.get()
            logger.info(f"[Worker]#{worker_name}: get a queue_data: {queue_data}")
            status_code: int | None = None
            while status_code != 200 or not status_code:
                logger.info(
                    f"[Worker]#{worker_name}: Sending a request for receive report"
                )
                status_code, response_data = await self._send_request(
                    api_method=HTTPMethods.GET,
                    payload=queue_data,
                    path=f"{APINamespaces.reports.value}/{queue_data.get('uuid')}",
                )
                status_code_description = StatusCodeDescription.reports_get.value.get(
                    str(status_code)
                )
                if status_code_description:
                    logger.info(
                        f"[Worker]#{worker_name}: {status_code_description} [{status_code}]"
                    )
                    await asyncio.sleep(1)
                else:
                    logger.warning(
                        f"[Worker]#{worker_name}: Something went wrong... [{status_code}]"
                    )
                    await asyncio.sleep(1)

            queue.task_done()
            await Loader.write_data_into_csv_file(queue_data)
            logging.info(f"#{worker_name}: task_done()")

    async def _request_report_creation(self):
        while True:
            _uuid = str(uuid.uuid4())
            queue_data = dict(timestamp=int(time.time()), uuid=_uuid)
            logger.info(f"[Report Creation] Sending a request for report creation")
            status_code, response_data = await self._send_request(
                api_method=HTTPMethods.PUT,
                path=f"{APINamespaces.reports.value}/{_uuid}",
            )
            status_code_description = StatusCodeDescription.reports_put_post.value.get(
                str(status_code)
            )
            if status_code == 201:
                logger.info(
                    f"[Report Creation] {status_code_description} [{status_code}]"
                )
                self._reports_queue.put_nowait(queue_data)
                await asyncio.sleep(60)
            elif status_code_description:
                logger.info(
                    f"[Report Creation] {status_code_description} [{status_code}]"
                )
            else:
                logger.warning(
                    f"[Report Creation] Something went wrong... [{status_code}]"
                )

    async def run(self):
        logger.info("Running program!")
        tasks = [self._request_report_creation()]
        for i in range(self._number_of_workers):
            task = self._reports_worker(self._reports_queue, f"Reports_{i}")
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    arguments = arguments_parser.parse_args().__dict__
    if arguments.get("run"):
        token = arguments.get("token")
        count_workers = arguments.get("count_workers")
        extractor = Extractor(token, count_workers)
        asyncio.run(extractor.run())
