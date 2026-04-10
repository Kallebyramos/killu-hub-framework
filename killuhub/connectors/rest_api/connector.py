"""
REST API connector for KilluHub.

Handles paginated JSON APIs with configurable pagination strategies.

Required config keys:
    url   — base endpoint URL

Optional:
    method          ("GET" | "POST", default "GET")
    headers         (dict)
    params          (dict) — query string params
    body            (dict) — request body for POST
    auth_type       ("bearer" | "basic" | None)
    auth_token      — used with auth_type "bearer"
    auth_user       — used with auth_type "basic"
    auth_password   — used with auth_type "basic"
    data_key        — JSON key that holds the records list (e.g. "data", "results")
    pagination      ("none" | "page" | "cursor" | "offset", default "none")
    page_param      — query param name for page number (default "page")
    page_size_param — query param name for page size (default "page_size")
    page_size       (int, default 100)
    cursor_key      — JSON key that holds next cursor (default "next_cursor")
    cursor_param    — query param name for cursor (default "cursor")
    max_pages       (int) — safety cap on pagination depth
"""
from typing import Any, Iterator

from killuhub.core.connector_interface import BaseConnector
from killuhub.core.config import ConnectorConfig


class RestApiConnector(BaseConnector):
    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self._session = None

    def connect(self) -> None:
        try:
            import requests
        except ImportError as e:
            raise ImportError(
                "requests is required for RestApiConnector. "
                "Install it with: pip install requests"
            ) from e

        import requests
        self._session = requests.Session()

        headers = self.config.get("headers", {})
        auth_type = self.config.get("auth_type")

        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.config.require('auth_token')}"
        elif auth_type == "basic":
            from requests.auth import HTTPBasicAuth
            self._session.auth = HTTPBasicAuth(
                self.config.require("auth_user"),
                self.config.require("auth_password"),
            )

        self._session.headers.update(headers)
        self._connected = True

    def extract(self) -> Iterator[dict[str, Any]]:
        if not self._connected:
            self.connect()

        pagination = self.config.get("pagination", "none")

        if pagination == "page":
            yield from self._paginate_by_page()
        elif pagination == "cursor":
            yield from self._paginate_by_cursor()
        elif pagination == "offset":
            yield from self._paginate_by_offset()
        else:
            yield from self._fetch_single()

    def _request(self, params: dict | None = None) -> Any:
        method = self.config.get("method", "GET").upper()
        url = self.config.require("url")
        base_params = dict(self.config.get("params") or {})
        if params:
            base_params.update(params)

        if method == "GET":
            resp = self._session.get(url, params=base_params)
        else:
            resp = self._session.post(
                url, params=base_params, json=self.config.get("body")
            )
        resp.raise_for_status()
        return resp.json()

    def _extract_records(self, data: Any) -> list[dict[str, Any]]:
        data_key = self.config.get("data_key")
        if data_key:
            return data.get(data_key, [])
        if isinstance(data, list):
            return data
        return [data]

    def _fetch_single(self) -> Iterator[dict[str, Any]]:
        yield from self._extract_records(self._request())

    def _paginate_by_page(self) -> Iterator[dict[str, Any]]:
        page_param = self.config.get("page_param", "page")
        size_param = self.config.get("page_size_param", "page_size")
        page_size = self.config.get("page_size", 100)
        max_pages = self.config.get("max_pages", None)
        page = 1

        while True:
            data = self._request({page_param: page, size_param: page_size})
            records = self._extract_records(data)
            if not records:
                break
            yield from records
            page += 1
            if max_pages and page > max_pages:
                break

    def _paginate_by_cursor(self) -> Iterator[dict[str, Any]]:
        cursor_key = self.config.get("cursor_key", "next_cursor")
        cursor_param = self.config.get("cursor_param", "cursor")
        max_pages = self.config.get("max_pages", None)
        cursor = None
        page = 0

        while True:
            params = {cursor_param: cursor} if cursor else {}
            data = self._request(params)
            records = self._extract_records(data)
            if not records:
                break
            yield from records
            cursor = data.get(cursor_key)
            if not cursor:
                break
            page += 1
            if max_pages and page >= max_pages:
                break

    def _paginate_by_offset(self) -> Iterator[dict[str, Any]]:
        size_param = self.config.get("page_size_param", "limit")
        page_size = self.config.get("page_size", 100)
        max_pages = self.config.get("max_pages", None)
        offset = 0
        page = 0

        while True:
            data = self._request({"offset": offset, size_param: page_size})
            records = self._extract_records(data)
            if not records:
                break
            yield from records
            offset += len(records)
            page += 1
            if max_pages and page >= max_pages:
                break

    def close(self) -> None:
        if self._session:
            self._session.close()
            self._session = None
            self._connected = False
