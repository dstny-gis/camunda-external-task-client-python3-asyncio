from http import HTTPStatus
from unittest import mock, IsolatedAsyncioTestCase
from unittest.mock import patch

from aioresponses import aioresponses

from camunda.client.external_task_client import ExternalTaskClient
from camunda.external_task.external_task import TaskResult, ExternalTask
from camunda.external_task.external_task_worker import ExternalTaskWorker


class ExternalTaskWorkerTest(IsolatedAsyncioTestCase):

    @aioresponses()
    @patch('camunda.client.external_task_client.ExternalTaskClient.complete')
    async def test_fetch_and_execute_calls_task_action_for_each_task_fetched(self, mocked: aioresponses, _):
        external_task_client = ExternalTaskClient(worker_id=0)
        resp_payload = [{
            "activityId": "anActivityId",
            "activityInstanceId": "anActivityInstanceId",
            "errorMessage": "anErrorMessage",
            "errorDetails": "anErrorDetails",
            "executionId": "anExecutionId",
            "id": "anExternalTaskId",
            "lockExpirationTime": "2015-10-06T16:34:42",
            "processDefinitionId": "aProcessDefinitionId",
            "processDefinitionKey": "aProcessDefinitionKey",
            "processInstanceId": "aProcessInstanceId",
            "tenantId": None,
            "retries": 3,
            "workerId": "aWorkerId",
            "priority": 4,
            "topicName": "createOrder",
            "variables": {
                "orderId": {
                    "type": "String",
                    "value": "1234",
                    "valueInfo": {}
                }
            }
        },
            {
                "activityId": "anActivityId",
                "activityInstanceId": "anActivityInstanceId",
                "errorMessage": "anErrorMessage",
                "errorDetails": "anotherErrorDetails",
                "executionId": "anExecutionId",
                "id": "anExternalTaskId",
                "lockExpirationTime": "2015-10-06T16:34:42",
                "processDefinitionId": "aProcessDefinitionId",
                "processDefinitionKey": "aProcessDefinitionKey",
                "processInstanceId": "aProcessInstanceId",
                "tenantId": None,
                "retries": 3,
                "workerId": "aWorkerId",
                "priority": 0,
                "topicName": "createOrder",
                "variables": {
                    "orderId": {
                        "type": "String",
                        "value": "3456",
                        "valueInfo": {}
                    }
                }
            }]
        mocked.post(external_task_client.get_fetch_and_lock_url(), status=HTTPStatus.OK, payload=resp_payload)

        worker = ExternalTaskWorker(worker_id=0)
        mock_action = mock.AsyncMock()
        task = ExternalTask({"id": "anExternalTaskId", "workerId": "aWorkerId", "topicName": "createOrder"})
        mock_action.return_value = TaskResult.success(task=task, global_variables={})

        await worker.fetch_and_execute("my_topic", mock_action)
        self.assertEqual(2, mock_action.call_count)

    @aioresponses()
    async def test_fetch_and_execute_raises_exception_if_task_action_raises_exception(self, mocked: aioresponses):
        external_task_client = ExternalTaskClient(worker_id=0)
        resp_payload = [{
            "activityId": "anActivityId",
            "activityInstanceId": "anActivityInstanceId",
            "errorMessage": "anErrorMessage",
            "errorDetails": "anErrorDetails",
            "executionId": "anExecutionId",
            "id": "anExternalTaskId",
            "lockExpirationTime": "2015-10-06T16:34:42",
            "processDefinitionId": "aProcessDefinitionId",
            "processDefinitionKey": "aProcessDefinitionKey",
            "processInstanceId": "aProcessInstanceId",
            "tenantId": None,
            "retries": 3,
            "workerId": "aWorkerId",
            "priority": 4,
            "topicName": "createOrder",
            "variables": {
                "orderId": {
                    "type": "String",
                    "value": "1234",
                    "valueInfo": {}
                }
            }
        }]
        mocked.post(external_task_client.get_fetch_and_lock_url(),
                    status=HTTPStatus.OK, payload=resp_payload)

        worker = ExternalTaskWorker(worker_id=0)
        mock_action = mock.Mock()
        mock_action.side_effect = Exception("error executing task action")

        with self.assertRaises(Exception) as exception_ctx:
            await worker.fetch_and_execute("my_topic", mock_action)

        self.assertEqual("error executing task action", str(exception_ctx.exception))

    @aioresponses()
    async def test_fetch_and_execute_raises_exception_if_no_tasks_found(self, mocked: aioresponses):
        external_task_client = ExternalTaskClient(worker_id=0)
        resp_payload = []
        mocked.post(external_task_client.get_fetch_and_lock_url(),
                    status=HTTPStatus.OK, payload=resp_payload)

        worker = ExternalTaskWorker(worker_id=0)
        mock_action = mock.Mock()
        process_variables = {"var1": "value1", "var2": "value2"}
        with self.assertRaises(Exception) as context:
            await worker.fetch_and_execute("my_topic", mock_action, process_variables)

        self.assertEqual(f"no External Task found for Topics: my_topic, Process variables: {process_variables}",
                         str(context.exception))

    @aioresponses()
    @patch('asyncio.sleep', return_value=None)
    async def test_fetch_and_execute_safe_raises_exception_sleep_is_called(self, mocked: aioresponses, mock_time_sleep):
        external_task_client = ExternalTaskClient(worker_id=0)
        mocked.post(external_task_client.get_fetch_and_lock_url(), status=HTTPStatus.INTERNAL_SERVER_ERROR)

        sleep_seconds = 100
        worker = ExternalTaskWorker(worker_id=0, config={"sleepSeconds": sleep_seconds})
        mock_action = mock.AsyncMock()

        await worker._fetch_and_execute_safe("my_topic", mock_action)

        self.assertEqual(0, mock_action.call_count)
        self.assertEqual(1, mock_time_sleep.call_count)
        mock_time_sleep.assert_called_with(sleep_seconds)
