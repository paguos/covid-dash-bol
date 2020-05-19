import os
import datetime
import locale

import requests
from bs4 import BeautifulSoup

locale.setlocale(locale.LC_TIME, "es_ES")


class CovidCrawler:
    def __init__(self, url: str):
        self.url = url

    def _fetch_website(self):
        resp = requests.get(self.url)
        return BeautifulSoup(resp.text, "html.parser")

    def overall_stats(self, date=datetime.datetime.now().date):
        soup = self._fetch_website()
        table_rows = soup.find_all(id="tablePreview")[0].find_all("tr")
        return {tr.th.text: tr.td.text for tr in table_rows}

    def department_stats(self):
        soup = self._fetch_website()
        table = soup.find_all(id="tablePreview")[1]
        headers = [h.text for h in table.thead.tr.find_all("th")]
        rows = [r.text for r in table.tbody.find("tr").find_all("td")]
        rows = [
            rows[i : i + len(headers)]
            for i in range(0, len(rows), len(headers))
        ]

        return [dict(zip(headers, r)) for r in rows]

    def website_date(self):
        soup = self._fetch_website()
        date_str = soup.select(".mapanuevos")[0].find("h5").text
        return datetime.datetime.strptime(
            date_str, "%A, %d de %B de %Y"
        ).date()


class DBController:

    def __init__(self, *kwargs):
        self.host = kwargs["host"]
        self.port = kwargs["port"]
        self.user = kwargs["user"]
        self.password = kwargs["password"]
        self.database = kwargs["database"]

    @staticmethod
    def store_overall_stats(overall_stats):

if __name__ == "__main__":
    WEBSITE_URL = os.getenv("WEBSITE_URL", "https://boliviasegura.gob.bo/")
    crawler = CovidCrawler(WEBSITE_URL)
    print(crawler.overall_stats())
    print(crawler.department_stats())
    print(crawler.website_date())
