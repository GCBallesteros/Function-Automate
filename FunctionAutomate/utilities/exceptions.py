import logging
from typing import Callable
from functools import wraps

import azure.functions as func

mainAlias = Callable[[func.HttpRequest], func.HttpResponse]

class HttpError(Exception):
    """Exception that also stores an HttpResponse describing the failutre.

    This exception is meant to be used in conjunction with the exception_as_response
    decorator.
    """
    def __init__(self, message: str, response: func.HttpResponse):
        super().__init__(message)
        self.response = response


def exceptions_as_response(main: mainAlias) -> mainAlias:
    """Decorate the main entry point of an Azure function.

    This enables a more elegant approach to non revarable errors which end in
    a HttpResponse to ease downstream client developmnet.
    """
    @wraps(main)
    def main_with_responses(req: func.HttpRequest) -> func.HttpResponse:
        try:
            return main(req)
        except HttpError as e:
            # HttpErrors exceptions automatically end executing and return
            # the response wrapped by the exception.
            logging.info(str(e))
            return e.response
        except Exception as e:
            logging.info(str(e))
            raise e

    return main_with_responses
