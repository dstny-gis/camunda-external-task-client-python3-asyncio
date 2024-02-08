import base64
import logging
from http import HTTPStatus

import aiohttp

from camunda.utils.auth_basic import AuthBasic
from camunda.utils.auth_bearer import AuthBearer
from camunda.utils.response_utils import raise_exception_if_not_ok
from camunda.utils.utils import join
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)

ENGINE_LOCAL_BASE_URL = "http://localhost:8080/engine-rest"


class EngineClient:

    def __init__(self, engine_base_url=ENGINE_LOCAL_BASE_URL, config=None):
        config = config if config is not None else {}
        self.config = config.copy()
        self.engine_base_url = engine_base_url

    def get_start_process_instance_url(self, process_key, tenant_id=None):
        if tenant_id:
            return f"{self.engine_base_url}/process-definition/key/{process_key}/tenant-id/{tenant_id}/start"
        return f"{self.engine_base_url}/process-definition/key/{process_key}/start"

    async def start_process(self, process_key, variables, tenant_id=None, business_key=None):
        """
        Start a process instance with the process_key and variables passed.
        :param process_key: Mandatory
        :param variables: Mandatory - can be empty dict
        :param tenant_id: Optional
        :param business_key: Optional
        :return: response json
        """
        url = self.get_start_process_instance_url(process_key, tenant_id)
        body = {
            "variables": Variables.format(variables)
        }
        if business_key:
            body["businessKey"] = business_key

        async with aiohttp.ClientSession() as session:
            response = await session.post(url, headers=self._get_headers(), json=body)
            await raise_exception_if_not_ok(response)
            return await response.json()

    async def get_process_instance(self, process_key=None, variables=frozenset([]), tenant_ids=frozenset([])):
        url = f"{self.engine_base_url}/process-instance"
        url_params = self.__get_process_instance_url_params(process_key, tenant_ids, variables)
        async with aiohttp.ClientSession() as session:
            response = await session.get(url, headers=self._get_headers(), params=url_params)
            await raise_exception_if_not_ok(response)
            return await response.json()

    @staticmethod
    def __get_process_instance_url_params(process_key, tenant_ids, variables):
        url_params = {}
        if process_key:
            url_params["processDefinitionKey"] = process_key
        var_filter = join(variables, ',')
        if var_filter:
            url_params["variables"] = var_filter
        tenant_ids_filter = join(tenant_ids, ',')
        if tenant_ids_filter:
            url_params["tenantIdIn"] = tenant_ids_filter
        return url_params

    @property
    def auth_basic(self) -> dict:
        if not self.config.get("auth_basic") or not isinstance(self.config.get("auth_basic"), dict):
            return {}
        token = AuthBasic(**self.config.get("auth_basic").copy()).token
        return {"Authorization": token}

    @property
    def auth_bearer(self) -> dict:
        if not self.config.get("auth_bearer") or not isinstance(self.config.get("auth_bearer"), dict):
            return {}
        token = AuthBearer(access_token=self.config["auth_bearer"]).access_token
        return {"Authorization": token}

    def _get_headers(self):
        headers = {
            "Content-Type": "application/json"
        }
        if self.auth_basic:
            headers.update(self.auth_basic)
        if self.auth_bearer:
            headers.update(self.auth_bearer)
        return headers

    async def correlate_message(self, message_name, process_instance_id=None, tenant_id=None, business_key=None, all=False,
                                process_variables=None, process_variables_local=None, correlation_keys=None, local_correlation_keys=None):
        """
        Correlates a message to the process engine to either trigger a message start event or
        an intermediate message catching event.
        :param message_name:
        :param process_instance_id:
        :param tenant_id:
        :param business_key:
        :param all:
        :param process_variables:
        :param process_variables_local:
        :param correlation_keys:
        :param local_correlation_keys:
        :return: response json
        """
        url = f"{self.engine_base_url}/message"
        body = {
            "messageName": message_name,
            "resultEnabled": True,
            "processVariables": Variables.format(process_variables) if process_variables else None,
            "processVariablesLocal": Variables.format(process_variables_local) if process_variables_local else None,
            "processInstanceId": process_instance_id,
            "tenantId": tenant_id,
            "withoutTenantId": not tenant_id,
            "businessKey": business_key,
            "all": True if all else None,
            "correlationKeys": correlation_keys,
            "localCorrelationKeys": local_correlation_keys,
        }

        if process_instance_id:
            body.pop("tenantId")
            body.pop("withoutTenantId")

        body = {k: v for k, v in body.items() if v is not None}

        async with aiohttp.ClientSession() as session:
            response = await session.post(url, headers=self._get_headers(), json=body)
            await raise_exception_if_not_ok(response)
            return await response.json()

    async def get_jobs(self,
                       offset: int,
                       limit: int,
                       tenant_ids=None,
                       with_failure=None,
                       process_instance_id=None,
                       task_name=None,
                       sort_by="jobDueDate",
                       sort_order="desc"):
        # offset starts with zero
        # sort_order can be "asc" or "desc

        url = f"{self.engine_base_url}/job"
        params = {
            "firstResult": offset,
            "maxResults": limit,
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }
        if process_instance_id:
            params["processInstanceId"] = process_instance_id
        if task_name:
            params["failedActivityId"] = task_name
        if with_failure:
            params["withException"] = "true"
        if tenant_ids:
            params["tenantIdIn"] = ','.join(tenant_ids)
        async with aiohttp.ClientSession() as session:
            response = await session.get(url, params=params, headers=self._get_headers())
            await raise_exception_if_not_ok(response)
            return await response.json()

    async def set_job_retry(self, job_id, retries=1):
        url = f"{self.engine_base_url}/job/{job_id}/retries"
        body = {"retries": retries}

        async with aiohttp.ClientSession() as session:
            response = await session.put(url, headers=self._get_headers(), json=body)
            await raise_exception_if_not_ok(response)
        return response.status == HTTPStatus.NO_CONTENT

    async def get_process_instance_variable(self, process_instance_id, variable_name, with_meta=False):
        url = f"{self.engine_base_url}/process-instance/{process_instance_id}/variables/{variable_name}"
        async with aiohttp.ClientSession() as session:
            response = await session.get(url, headers=self._get_headers())
            await raise_exception_if_not_ok(response)
            resp_json = await response.json()

            url_with_data = f"{url}/data"
            response = await session.get(url_with_data, headers=self._get_headers())
            await raise_exception_if_not_ok(response)
            decoded_value = base64.encodebytes(await response.content.read()).decode("utf-8")

        if with_meta:
            return dict(resp_json, value=decoded_value)
        return decoded_value

    async def correlate_message_manually(self, message_name, process_instance_id=None, tenant_id=None, business_key=None, variables=None):
        """
        Manually correlates a message to the process engine to trigger a message receive task.
        :param message_name:
        :param process_instance_id:
        :param tenant_id:
        :param business_key:
        :param variables:
        :return: response json
        """
        matches = 0

        async with aiohttp.ClientSession() as session:
            # Fetch a list of executions that are waiting messages based on the message name
            url = f"{self.engine_base_url}/execution"
            body = {
                "messageEventSubscriptionName": message_name,
                "active": "true",  # Only active executions
                "tenantIdIn": tenant_id,
                "businessKey": business_key,
                "processInstanceId": process_instance_id,
            }
            body = {k: v for k, v in body.items() if v is not None}

            response = await session.post(url, headers=self._get_headers(), json=body)
            await raise_exception_if_not_ok(response)
            executions = await response.json()

            if not executions:
                logger.info('No executions found for message %s', message_name)
                return matches

            # Loop over all executions and fetch their input variables
            for execution in executions:
                logger.debug('Checking execution %s of process instance %s...', execution['id'], execution['processInstanceId'])
                url = f"{self.engine_base_url}/execution/{execution['id']}/localVariables"
                params = {
                    "deserializeValues": 'false'
                }
                response = await session.get(url, headers=self._get_headers(), params=params)
                await raise_exception_if_not_ok(response)
                input_variables = await response.json()

                # Check if the input variables match the expected variables
                if self.__match_variables(input_variables, variables or {}):
                    logger.info('Match found for execution %s of process instance %s: input variables %s MATCHES with message %s', execution['id'],
                                execution['processInstanceId'], input_variables, variables)

                    # If they match, trigger the message
                    url = f"{self.engine_base_url}/execution/{execution['id']}/messageSubscriptions/{message_name}/trigger"
                    body = {
                        "variables": {}  # variables,
                    }
                    response = await session.post(url, headers=self._get_headers(), json=body)
                    await raise_exception_if_not_ok(response)

                    # Increment the matches counter
                    matches += 1
                else:
                    logger.info('No match found for execution %s of process instance %s: input variables %s DON\'T MATCH with message %s', execution['id'],
                                execution['processInstanceId'], input_variables, variables)

        return matches

    @staticmethod
    def __match_variables(input_variables, variables):
        if not input_variables:
            return True
        for key, value in input_variables.items():
            # logger.debug('Checking input variable %s = %s...', key, value)

            if key not in variables:
                # logger.debug('No match found for key %s (not found)', key)
                return False
            # TODO: type conversion
            if variables[key] != value["value"]:
                # logger.debug('No match found for key %s with value %s', key, value)
                return False
        return True
