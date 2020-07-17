"""Generate an Azure table entry that can be used to start an ADF pipeline.

This function is used in conjunction with PipelineRestart which read the token
parameter from its incoming request to find the table entry that has the details
about the ADF pipeline that needs to be started.

The incoming request to this function must contain the following parameters:
- data: Contains contextual information for the restarted pipeline.
- resource_group: Resource group where the ADF is located.
- factory_name: Name of the ADF.
- pipeline_name: Name of pipeline that needs to be triggered on restart.
- expiration_time: Number of seconds until expiration of the restart parameters.
- web_path:
- share_name:

Author: Guillem Ballesteros
"""
import json
import os
import secrets
from typing import Any, Dict, Union

from __app__.utilities import exceptions
from __app__.utilities import utilities

import azure.functions as func
from azure.cosmosdb.table.models import Entity

def get_pipeline_params(req: func.HttpRequest) -> Dict[str, Union[int, str]]:
    pipeline_params = {
        k: utilities.get_param(req, k)
        for k in ["factory_name", "resource_group", "pipeline_name", "expiration_time"]
    }

    # All pipeline parameters are mandatory
    if None in [pipeline_params[k] for k in pipeline_params]:
        msg = (
            "One of ['resource_group', 'factory_name', 'pipeline_name', 'expiration_time'] was not found.",
        )
        raise exceptions.HttpError(msg, func.HttpResponse(msg, status_code=500))

    return pipeline_params


def get_notification_web_params(req: func.HttpRequest) -> Dict[str, str]:
    web_params = {k: utilities.get_param(req, k) for k in ["web_path", "share_name"]}

    # All web parameters are mandatory
    if None in [web_params[k] for k in web_params]:
        msg = ("One of ['web_path', 'share_name'] was not found.",)
        raise exceptions.HttpError(msg, func.HttpResponse(msg, status_code=500))

    return web_params


def prepare_pipeline_data(
    partition_key: str,
    token: str,
    pipeline_params: Dict[str, Union[int, str]],
    notification_web_params: Dict[str, str],
    data: Any,
) -> Entity:
    pipeline_data = Entity()
    pipeline_data.PartitionKey = partition_key
    pipeline_data.RowKey = token
    pipeline_data.factory_name = pipeline_params["factory_name"]
    pipeline_data.resource_group = pipeline_params["resource_group"]
    pipeline_data.pipeline_name = pipeline_params["pipeline_name"]
    pipeline_data.expiration_time = pipeline_params["expiration_time"]
    pipeline_data.data = json.dumps(data)
    pipeline_data.acted_upon = (
        0  # to be marked as read (1) once the pipeline has restarted
    )
    pipeline_data.web_path = notification_web_params["web_path"]
    pipeline_data.share_name = notification_web_params["share_name"]

    return pipeline_data


@exceptions.exceptions_as_response
def main(req: func.HttpRequest) -> func.HttpResponse:
    partition_key = "PauseData"
    target_table = "PipelinePauseData"
    table_service = utilities.setup_table_service(
        os.environ["AzureWebJobsStorage"], target_table,
    )

    # Gather all the data we need for the table entry
    data = utilities.get_param(req, "data")
    pipeline_params = get_pipeline_params(req)
    notification_web_params = get_notification_web_params(req)
    token = secrets.token_urlsafe(64)

    pipeline_data = prepare_pipeline_data(
        partition_key, token, pipeline_params, notification_web_params, data
    )
    table_service.insert_entity(target_table, pipeline_data)

    return func.HttpResponse(json.dumps({"token": token}))
