import backoff
import requests
from singer import get_logger

LOGGER = get_logger()


class TwitterClientError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class TwitterClient400Error(TwitterClientError):
    pass


class TwitterClient401Error(TwitterClientError):
    pass


class TwitterClient403Error(TwitterClientError):
    pass


class TwitterClient404Error(TwitterClientError):
    pass


class TwitterClient429Error(TwitterClientError):
    pass


class TwitterClient5xxError(TwitterClientError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        'raise_exception': TwitterClient400Error,
        'message': 'Bad Request'
    },
    401: {
        'raise_exception': TwitterClient401Error,
        'message': 'Unauthorized'
    },
    403: {
        'raise_exception': TwitterClient403Error,
        'message': 'Forbidden'
    },
    404: {
        'raise_exception': TwitterClient404Error,
        'message': 'Not Found'
    },
    429: {
        'raise_exception': TwitterClient429Error,
        'message': 'API limit has been reached'
    },
    500: {
        'raise_exception': TwitterClient5xxError,
        'message': 'Internal Server Error',
    },
    503: {
        'raise_exception': TwitterClient5xxError,
        'message': 'Service Unavailable',
    },
}


def raise_for_error(resp: requests.Response):
    """
    Raises the associated response exception.
    Takes in a response object, checks the status code, and throws the associated
    exception based on the status code.
    :param resp: requests.Response object
    """
    try:
        resp.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = resp.status_code
            client_exception = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {})
            exc = client_exception.get('raise_exception', TwitterClientError)
            error_message = resp.json().get('errors', [{}])[0].get('message')
            message = error_message or client_exception.get('message', 'Client Error')

            raise exc(message, resp) from None

        except (ValueError, TypeError):
            raise TwitterClientError(error) from None


def retry_after_wait_gen():
    """
    Returns a generator that is passed to backoff decorator to indicate how long
    to backoff for in seconds.
    """
    max_value = 300
    n = 10
    while n <= max_value:
        yield n
        n += 10


class Client:

    def __init__(self, api_token: str):
        self._api_token = api_token
        self._base_url = "https://api.twitter.com"
        self._session = requests.Session()
        self._headers = {"Authorization": f"Bearer {api_token}"}

    def _build_url(self, endpoint: str) -> str:
        """
        Builds the URL for the API request.
        :param endpoint: The API URI (resource)
        :return: The full API URL for the request
        """
        return f"{self._base_url}{endpoint}"

    def _get(self, url, headers=None, params=None, data=None):
        """
        Wraps the _make_request function with a 'GET' method
        """
        return self._make_request(url, method='GET', headers=headers, params=params, data=data)

    def _post(self, url, headers=None, params=None, data=None):
        """
        Wraps the _make_request function with a 'POST' method
        """
        return self._make_request(url, method='POST', headers=headers, params=params, data=data)

    @backoff.on_exception(retry_after_wait_gen, TwitterClient429Error, jitter=None, max_tries=10)
    def _make_request(self, url, method, headers=None, params=None, data=None) -> dict:
        """
        Makes the API request.
        :param url: The full API url
        :param method: The API request method
        :param headers: The headers for the API request
        :param params: The querystring params passed to the API
        :param data: The data passed to the body of the request
        :return: A dictionary representing the response from the API
        """

        with self._session as session:
            response = session.request(method, url, headers=headers, params=params, data=data)

            if response.status_code != 200:
                LOGGER.critical(f"error: {response.json()}")
                raise_for_error(response)
                return None

            return response.json()

    def get(self, endpoint, params=None):
        """
        Takes the endpoint and builds and makes a 'GET' request
        to the API.
        """
        url = self._build_url(endpoint)
        return self._get(url, headers=self._headers, params=params)

    def get_recent_tweets(self, params=None):
        endpoint = "/2/tweets/search/recent"
        return self.get(endpoint, params)
