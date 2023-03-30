from aiohttp import ClientSession

import asyncio
import logging
import time
import uuid
from enum import Enum

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


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
    }


class Extractor:
    URL: str = "https://analytics.maximum-auto.ru/vacancy-test/api/v0.1"
    # URL = "https://dummyjson.com/products"
    _token: str = "f105a919-6875-4972-8331-f1906a77df4"
    _reports_queue = asyncio.Queue()

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
            async with session.request(**kwargs) as resposne:
                if resposne.status == 200:
                    response_data = await resposne.json()
                else:
                    response_data = await resposne.text()
                return resposne.status, response_data

    async def _reports_worker(self, queue, worker_name: str | None = None):
        logger.info(f"Running worker #{worker_name}")
        while True:
            queue_data: dict = await queue.get()
            logger.info(f"[Worker] #{worker_name}: get a queue_data: {queue_data}")
            status_code: int | None = None
            while status_code != 200 or not status_code:
                logger.info(
                    f"[Worker]#{worker_name}: Sending a request for receive report"
                )
                status_code, response_data = await self._send_request(
                    api_method=HTTPMethods.GET,
                    payload=queue_data,
                    path=f"{APINamespaces.reports.value}/{queue_data.get('id')}",
                )
                # logger.info(
                #     f"[Worker]#{worker_name}: Sent a request for receive report [{status_code}]"
                # )
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
            # logger.info(f"[Report Creation] Sent a request for report creation [{status_code}]")
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
        for i in range(2):
            task = self._reports_worker(self._reports_queue, f"Reports_{i}")
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)


extractor = Extractor()
asyncio.run(extractor.run())
