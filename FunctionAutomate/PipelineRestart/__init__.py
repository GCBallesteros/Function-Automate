"""

Author: Guillem Ballesteros
"""
import datetime
import logging
import os

from __app__.utilities import exceptions
from __app__.utilities import utilities

import azure.functions as func
from azure.common import AzureMissingResourceHttpError
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.fileshare import ShareFileClient

import pytz

# ToDo
#
#
# in ADF you also need to create a new Dataset for table storage
# syntax for queries in https://docs.microsoft.com/en-us/rest/api/storageservices/Query-Operators-Supported-for-the-Table-Service?redirectedfrom=MSDN
#
# Docs for SDK
# https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory.datafactorymanagementclient?view=azure-python
# Factory Docs
# https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory.models.factory?view=azure-python
# https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory.operations.pipelinesoperations?view=azure-python


def check_if_expired(timestamp: datetime.datetime, expiration_time: int) -> bool:
    """
    Check if a timestamp is older than the current time.

    The current time is obtained through a call to datetime.now() and localized to
    UTC as that is what the Azure cloud uses for its timestamps.

    Parameters
    ----------
    timestamp
        UTC time-zone aware timestamp.
    expiration_time
        Time to expiration in seconds.

    Returns
    -------
    Has the timestamp expired?
    """
    expiration_timestamp = timestamp + datetime.timedelta(seconds=expiration_time)
    current_time = pytz.utc.localize(datetime.datetime.now())

    has_expired = current_time > expiration_timestamp
    return has_expired


def restart_pipeline(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    pipeline_name: str,
    token: str,
):
    pipelines = adf_client.pipelines.list_by_factory(
        resource_group_name=resource_group, factory_name=factory_name,
    )

    available_pipelines = [pipeline.name for pipeline in pipelines]
    if pipeline_name not in available_pipelines:
        msg = f"{pipeline_name} is not available in the data factory."
        raise exceptions.HttpError(msg, func.HttpResponse(msg, status_code=500))

    run_response = adf_client.pipelines.create_run(
        resource_group, factory_name, pipeline_name, parameters={"token": token},
    )

    return run_response


@exceptions.exceptions_as_response
def main(req: func.HttpRequest) -> func.HttpResponse:
    target_table = "PipelinePauseData"
    token = utilities.get_param(req, "token")

    table_service = utilities.setup_table_service(
        os.environ["AzureWebJobsStorage"], target_table,
    )

    # Since we can't use authentication for the API we will check as
    # soon as possible if the token for the pipeline restart is valid.
    # if it is not we halt execution and return a 500 code.
    try:
        paused_pipeline = table_service.get_entity(
            table_name=target_table, partition_key="PauseData", row_key=token
        )
    except AzureMissingResourceHttpError as e:
        raise exceptions.HttpError(
            str(e),
            func.HttpResponse(str(e), status_code=500)
        )

    # acted_upon monitors if a token has already been used. We use it here to
    # block the second and further attempts at restarting.
    acted_upon = paused_pipeline["acted_upon"]

    has_expired = check_if_expired(
        paused_pipeline["Timestamp"], paused_pipeline["expiration_time"],
    )

    if not acted_upon and not has_expired:
        logging.info(token)

        # DefaultAzureCredential does not work when manipulating ADF. It will
        # complain about a missing session method.
        # Remember to give the contributor role to the application.
        # Azure Portal -> Subscriptions -> IAM roles
        credentials = ServicePrincipalCredentials(
            client_id=os.environ["AZURE_CLIENT_ID"],
            secret=os.environ["AZURE_CLIENT_SECRET"],
            tenant=os.environ["AZURE_TENANT_ID"],
        )

        subscription_id = os.environ["subscription_id"]
        adf_client = DataFactoryManagementClient(credentials, subscription_id)
        logging.info(adf_client)

        # The restart data is accessed via a lookup activity from within ADF
        run_response = restart_pipeline(
            adf_client=adf_client,
            resource_group=paused_pipeline["resource_group"],
            factory_name=paused_pipeline["factory_name"],
            pipeline_name=paused_pipeline["pipeline_name"],
            token=token,
        )
        logging.info(run_response)

        # After running acted_upon is set to 1
        paused_pipeline["acted_upon"] = 1
        table_service.update_entity(target_table, paused_pipeline)

        # Retrieve and display success webpage.
        confirmation_site = (
            ShareFileClient.from_connection_string(
                conn_str=os.environ["AzureWebJobsStorage"],
                share_name=paused_pipeline["share_name"],
                file_path=paused_pipeline["web_path"],
            )
            .download_file()
            .readall()
            .decode("utf-8")
        )

        return func.HttpResponse(confirmation_site, mimetype="text/html")

    else:  # already acted_upon or expired
        return func.HttpResponse("Invalid token.", status_code=500,)
