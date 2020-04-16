"""Compose text using Jinja2.

Composes a text using a Jinja2 template stored in the Function App's storage
account.

The incoming request is expected to have the following 3 parameters:
- template_parameters
- template_file
- share_name

Requires the following env variables:
- AzureWebJobsStorage

Author: Guillem Ballesteros
"""
import json
import os

import azure.functions as func
from azure.storage.fileshare import ShareFileClient

from jinja2 import Template

from __app__.utilities import utilities


def get_template(conn_str: str, share_name: str, template_path: str) -> Template:
    """Retrieve Jinja2 template from Azure File Share.

    Parameters
    ----------
    conn_str
        Connection string to the storage account. Typically stored in an env
        variable.
    share_name
        Name of the file share in the storage account where the template file
        is kept.
    template_path
        Full path to the template file relative to the root of the file share.
    """
    data = ShareFileClient.from_connection_string(
        conn_str=conn_str, share_name=share_name, file_path=template_path,
    ).download_file()

    template = Template(data.readall().decode("utf-8"))

    return template


def main(req: func.HttpRequest) -> func.HttpResponse:
    template_parameters = utilities.get_param(req, "template_parameters")
    template_file = utilities.get_param(req, "template_file")
    share_name = utilities.get_param(req, "share_name")

    template = get_template(
        conn_str=os.environ["AzureWebJobsStorage"],
        share_name=share_name,
        template_path=template_file,
    )

    completed_template = template.render(template_parameters)

    return func.HttpResponse(json.dumps({"output_text": completed_template}))
