"""Common utility functions.

Author: Guillem Ballesteros
"""
from typing import Any, Dict, List, Tuple

from __app__.utilities import exceptions

import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService


def get_param(request: func.HttpRequest, param_name: str) -> Any:
    """Extract parameter from incoming request.

    Parameters
    ----------
    request
        Incoming request
    param_name
        Name of the parameter that it will attempt to extract.

    Return
    ------
    The named parameter or None if it could not be found.
    """
    param = request.params.get(param_name)
    if not param:
        try:
            req_body = request.get_json()
        except ValueError:
            param = None
        else:
            param = req_body.get(param_name)

    if not param:
        return None
    else:
        return param


def extract_storage_parameters(conn_str: str) -> Dict[str, str]:
    """Break an Azure connection string into its componenets.

    Azure connection strings are a single line representation of everything that
    is needed to connect to an Azure storage account. They have (28 April) the
    following format:
    DefaultEndpointsProtocol=https;AccountName=somename;AccountKey=CZn1zhwx...LnT4DrrTGn=;EndpointSuffix=core.windows.net

    Parameters
    ----------
    conn_str
        Connectoin string to an Azure storage account

    Returns
    -------
    Dictionary with the following parameters:
    - DefaultEndpointsProtocol
    - AccountName
    - AccountKey
    - EndpointSuffix

    """
    name_parameter_pairs: List[Tuple[str, str]] = [
        (x[0], x[1])
        for x in [key_value.split("=", 1) for key_value in conn_str.split(";")]
    ]
    params: Dict[str, str] = dict(name_parameter_pairs)

    return params


def setup_table_service(conn_str: str, target_table: str) -> TableService:
    """Setup a Table Service for a the target_table.

    Parameters
    ----------
    conn_str
        Connection string to an Azure storage account
    target_table
        Name of the table we want to create the table service for.

    Raise
    -----
    Raises an exceptions.HttpError if the table was not found in the storage
    account.
    """
    storage_params = extract_storage_parameters(conn_str)
    table_service = TableService(
        account_name=storage_params["AccountName"],
        account_key=storage_params["AccountKey"],
    )

    if not table_service.exists(target_table):
        msg = f"Table {target_table} to store request info did not exist."
        raise exceptions.HttpError(
            msg, func.HttpResponse(msg, status_code=500),
        )

    return table_service
