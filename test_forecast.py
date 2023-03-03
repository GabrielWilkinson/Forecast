from forecast import *
import pytest
import requests
from unittest.mock import Mock

mock = Mock()
print(mock)

def test_error_is_raised_if_request_unsuccessful():
    with pytest.raises(requests.exceptions.HTTPError):
        api_request(100, 50)
