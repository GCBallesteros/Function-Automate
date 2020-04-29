"""

Author: Guillem Ballesteros
"""
import datetime
import logging
import os

from __app__.utilities import exceptions
from __app__.utilities import utilities

import azure.functions as func
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.fileshare import ShareFileClient

import pytz

# ToDo
#
# retun failures when DF wasn't there and log it
# Build the pipeline that will do the lookup activity
# we just pass the token which is read by ADF
#
# what if the token has been used
# or if it does not exist
# in ADF you also need to create a new Dataset for table storage
# syntax for queries in https://docs.microsoft.com/en-us/rest/api/storageservices/Query-Operators-Supported-for-the-Table-Service?redirectedfrom=MSDN
#
# error checks


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

    # Retrieve from the table the details required to run the second halve of
    # the pipeline.
    paused_pipeline = table_service.get_entity(
        table_name=target_table, partition_key="PauseData", row_key=token
    )
    # what happens if token not there?

    # acted_upon monitors if a token has already been used. We use it here to
    # block further restarts.
    acted_upon = paused_pipeline["acted_upon"]

    has_expired = check_if_expired(
        paused_pipeline["Timestamp"], paused_pipeline["expiration_time"],
    )

    if not acted_upon and not has_expired:
        logging.info(token)

        # DefaultAzureCredential does not work when manipulating ADF.
        # If you try it, it will complain about a missing session method.
        # You will also gave to give the contributor role to application.
        # Azure Portal -> Subscriptions -> IAM roles
        credentials = ServicePrincipalCredentials(
            client_id=os.environ["AZURE_CLIENT_ID"],
            secret=os.environ["AZURE_CLIENT_SECRET"],
            tenant=os.environ["AZURE_TENANT_ID"],
        )

        # Docs for SDK
        # https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory.datafactorymanagementclient?view=azure-python
        # Factory Docs
        # https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory.models.factory?view=azure-python
        subscription_id = os.environ["subscription_id"]
        adf_client = DataFactoryManagementClient(credentials, subscription_id)
        logging.info(adf_client)

        # The restart data is accessed via a lookup activity from within
        # ADF
        run_response = restart_pipeline(
            adf_client=adf_client,
            resource_group=paused_pipeline["resource_group"],
            factory_name=paused_pipeline["factory_name"],
            pipeline_name=paused_pipeline["pipeline_name"],
            token=token,
        )
        logging.info(run_response)
        # acted_upon is set to 1 by the pipeline on success.

        # https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory.operations.pipelinesoperations?view=azure-python

        # The confirmation of restart websites are stored in the storage
        # account for extra flexibility.
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
