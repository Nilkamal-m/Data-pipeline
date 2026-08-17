"""
HTTP GET Client Wrapper for REST API Connectors.

STRICT POLICY: No HTTP POST requests are performed. All calls execute via HTTP GET.
"""

from .http_client import HTTPClient


class OAuth2Client:
    """
    HTTP GET-only request executor.
    """

    @staticmethod
    def make_authenticated_request(
        url: str,
        access_token: str,
        headers: None = None,
        method: str = 'GET',
        body: None = None,
        max_retries: int = 3,
        refresh_token_fn: None = None
    ):
        """
        Executes an HTTP GET request with Authorization: Bearer <access_token>.
        STRICTLY forces method='GET'.
        """
        secret_dict = {'bearer_token': access_token}
        return HTTPClient.get(url=url, secret_dict=secret_dict, headers=headers, max_retries=max_retries)
