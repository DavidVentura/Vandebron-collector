import base64
import calendar
import json
import uuid

import requests

from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from dynaconf import settings


@dataclass
class UserInfo:
    user_id: str
    org_id: str


@dataclass
class Connection:
    market_segment: str
    conn_id: str


class Vandebron:
    class URLs:
        AUTH = "https://vandebron.nl/auth/realms/vandebron/protocol/openid-connect/auth"
        TOKEN = "https://vandebron.nl/auth/realms/vandebron/protocol/openid-connect/token"
        USER_INFO = "https://mijn.vandebron.nl/api/authentication/userinfo"
        ENERGY_CONSUMERS = "https://mijn.vandebron.nl/api/v1/energyConsumers/{org_id}"
        USAGE = "https://mijn.vandebron.nl/api/consumers/{user_id}/connections/{conn_id}/usage"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self._session = requests.Session()
        self._token: Optional[str] = None

    def _get_login_url(self) -> str:
        state = str(uuid.uuid4())
        nonce = str(uuid.uuid4())

        params = {
            "client_id": "website",
            "redirect_uri": "https://mijn.vandebron.nl/",
            "state": state,
            "response_mode": "fragment",
            "response_type": "code",
            "scope": "openid",
            "nonce": nonce,
        }

        r = self._session.get(Vandebron.URLs.AUTH, params=params)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, features="html.parser")
        f = soup.find("form")
        url = str(f.attrs["action"])
        return url

    @property
    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    def _get_auth_code(self) -> str:
        login_url = self._get_login_url()
        r = self._session.post(
            login_url,
            data={"username": self.username, "password": self.password, "login": "Log in"},
            allow_redirects=False,
        )
        r.raise_for_status()

        loc_url = urlparse(r.headers["Location"])
        h = parse_qs(loc_url.fragment)
        return h["code"][0]

    def _get_token(self) -> str:
        auth_code = self._get_auth_code()
        data = {
            "grant_type": "authorization_code",
            "client_id": "website",
            "code": auth_code,
            "redirect_uri": "https://mijn.vandebron.nl/",
        }
        r = self._session.post(Vandebron.URLs.TOKEN, data=data)

        return str(r.json()["access_token"])

    def login(self) -> None:
        self._token = self._get_token()
        self.user = v._get_user()

    def _get_user(self) -> UserInfo:
        r = self._session.get(Vandebron.URLs.USER_INFO, headers=self._headers)
        data = r.json()
        userinfo = UserInfo(data["id"], data["organizationId"])
        return userinfo

    def get_connections(self) -> List[Connection]:
        r = self._session.get(Vandebron.URLs.ENERGY_CONSUMERS.format(org_id=self.user.org_id), headers=self._headers)
        r.raise_for_status()
        org_data = r.json()
        assert len(org_data["shippingAddresses"]) == 1
        connections = []
        for con in org_data["shippingAddresses"][0]["connections"]:
            connections.append(Connection(con["marketSegment"], con["connectionId"]))
        return connections

    def get_connection_usage(self, c: Connection, start_date: date, end_date: date) -> Dict[str, Any]:
        url = Vandebron.URLs.USAGE.format(user_id=self.user.user_id, conn_id=c.conn_id)
        r = self._session.get(
            url,
            params={"resolution": "Days", "startDate": start_date.isoformat(), "endDate": end_date.isoformat()},
            headers=self._headers,
        )
        r.raise_for_status()
        return {**r.json(), "market": c.market_segment}


def _month_range(d: date) -> Tuple[date, date]:
    _, eom = calendar.monthrange(d.year, d.month)
    return d.replace(day=1), d.replace(day=eom)


def output_print_json(data: List[Dict[str, Any]]) -> None:
    print(json.dumps(data, indent=4))


def output_influxdb(data: List[Dict[str, Any]]) -> None:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS

    client = InfluxDBClient(url=settings.INFLUXDB.URL, token=settings.INFLUXDB.TOKEN, org=settings.INFLUXDB.ORG)
    bucket = "sensordata"
    write_api = client.write_api(write_options=SYNCHRONOUS)
    SEC_TO_NANO = 10**9

    for conn in data:
        for v in conn["values"]:
            item = {
                "time": int(datetime.fromisoformat(v["time"].replace("Z", "")).timestamp()),
                "consumptionPeak": v["consumptionPeak"],
                "consumptionOffPeak": v["consumptionOffPeak"],
            }
            point = (
                Point(conn["market"])
                .tag("type", "consumptionPeak")
                .field("value", item["consumptionPeak"])
                .time(item["time"] * SEC_TO_NANO)
            )
            write_api.write(bucket=bucket, org=settings.INFLUXDB.ORG, record=point)
            point = (
                Point(conn["market"])
                .tag("type", "consumptionOffPeak")
                .field("value", item["consumptionOffPeak"])
                .time(item["time"] * SEC_TO_NANO)
            )
            write_api.write(bucket=bucket, org=settings.INFLUXDB.ORG, record=point)


v = Vandebron(settings.USERNAME, settings.PASSWORD)
v.login()
start, _ = _month_range(date.today().replace(month=10))
end = date.today()
data = [v.get_connection_usage(conn, start, end) for conn in v.get_connections()]

if settings.OUTPUT == 'influxdb':
    print("Pushing to influxdb")
    output_influxdb(data)
else:
    output_print_json(data)
