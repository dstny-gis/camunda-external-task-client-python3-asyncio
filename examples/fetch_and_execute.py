import logging

import asyncio

from camunda.external_task.external_task_worker import ExternalTaskWorker
from examples.task_handler_example import handle_task

logger = logging.getLogger(__name__)

default_config = {
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 0,
    "isDebug": True,
}


async def main():
    configure_logging()
    topics = ["PARALLEL_STEP_1", "PARALLEL_STEP_2", "COMBINE_STEP"]
    for index, topic in enumerate(topics):
        await ExternalTaskWorker(worker_id=index, config=default_config) \
            .fetch_and_execute(topic_names=topic, action=handle_task, process_variables={"strVar": "hello"})


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


if __name__ == '__main__':
    asyncio.run(main())
