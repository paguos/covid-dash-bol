import os
import datetime
import locale

import psycopg2
import requests
from bs4 import BeautifulSoup
from loguru import logger

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
    def __init__(self, **kwargs):
        self.host = kwargs["host"]
        self.port = kwargs["port"]
        self.user = kwargs["user"]
        self.password = kwargs["password"]
        self.database = kwargs["database"]

    def _connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )

    def save_date(self, date: datetime.date):
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM date_dim WHERE date_date = '{date}'"
                )

                query_result = cur.fetchone()
                if query_result:
                    return query_result[0]
                cur.execute(
                    f"INSERT INTO date_dim (date_date) VALUES('{date}') RETURNING date_sk"
                )
                date_sk = cur.fetchone()[0]
                conn.commit()
                return date_sk

    def save_status(self, status: str):
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM status_dim WHERE status_name = '{status}'"
                )

                query_result = cur.fetchone()
                if query_result:
                    return query_result[0]
                cur.execute(
                    f"INSERT INTO status_dim (status_name) VALUES('{status}') RETURNING status_sk"
                )
                status_sk = cur.fetchone()[0]
                conn.commit()
                return status_sk

    def save_department(self, department: str):
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM department_dim WHERE department_name = '{department}'"
                )

                query_result = cur.fetchone()
                if query_result:
                    return query_result[0]
                cur.execute(
                    f"INSERT INTO department_dim (department_name) VALUES('{department}') RETURNING department_sk"
                )
                deparment_sk = cur.fetchone()[0]
                conn.commit()
                return deparment_sk

    def save_overall_fact(self, date_sk: int, status_sk: int, count: int):
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO overall_facts (date_sk, status_sk, count) VALUES({date_sk}, {status_sk}, {count}) "
                    f"ON CONFLICT (date_sk, status_sk) DO "
                    f"UPDATE SET count = EXCLUDED.count"
                )
                conn.commit()

    def save_department_fact(
        self, date_sk: int, department_sk: int, status_sk: int, count: int
    ):
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO department_facts (date_sk, department_sk, status_sk, count) "
                    f"VALUES({date_sk}, {department_sk}, {status_sk}, {count}) "
                    f"ON CONFLICT (date_sk, department_sk, status_sk) DO "
                    f"UPDATE SET count = EXCLUDED.count"
                )
                conn.commit()


if __name__ == "__main__":
    DB_HOST = os.getenv("DB_HOST", "kandula.db.elephantsql.com")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "ttlrxfae")
    DB_PWD = os.getenv("DB_PWD", "dDY2dxzDOmtcmnQP5VJogizDDNSIxtJn")
    DB_DATABASE = os.getenv("DB_DATABASE", "ttlrxfae")
    WEBSITE_URL = os.getenv("WEBSITE_URL", "https://boliviasegura.gob.bo/")

    crawler = CovidCrawler(WEBSITE_URL)
    dbController = DBController(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PWD,
        database=DB_DATABASE,
    )
    # print(crawler.overall_stats())
    # print(crawler.department_stats())
    logger.info("Crawling website date ... ")
    date_date = crawler.website_date()
    date_sk = dbController.save_date(date_date)
    logger.debug(f"Date '{date_date}' ID '{date_sk}'")
    logger.info("Crawling website date ... done!")

    logger.info("Crawling overall stats ... ")
    overall_stats = crawler.overall_stats()

    for status, count in overall_stats.items():
        status_sk = dbController.save_status(status)
        logger.debug(f"Status '{status}' has the SK '{status_sk}'")
        if isinstance(count, str):
            count = count.replace(",", "")
        dbController.save_overall_fact(date_sk, status_sk, count)
    logger.info("Crawling overall stats ... done!")

    logger.info("Crawling deparment stats ... ")
    department_stats = crawler.department_stats()
    for department in department_stats:
        # print(department)
        department_sk = dbController.save_department(
            department["Departamento"]
        )
        logger.debug(
            f"Department '{department['Departamento']}' has the SK '{department_sk}'"
        )
        department = {
            "Confirmados": department["Acumulado"],
            "Decesos": department["Decesos"],
            "Recuperados": department["Recuperados"],
        }

        for status, count in department.items():
            status_sk = dbController.save_status(status)
            if isinstance(count, str):
                count = count.replace(",", "")
            dbController.save_department_fact(
                date_sk, department_sk, status_sk, count
            )
    logger.info("Crawling deparment stats ... done!")
