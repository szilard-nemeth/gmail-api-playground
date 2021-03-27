import logging

from tabulate import tabulate

LOG = logging.getLogger(__name__)


class ResultPrinter:
    def __init__(self, data, headers):
        self.data = data
        self.headers = headers

    def print_table(self):
        LOG.info("Printing result table: %s", tabulate(self.data, self.headers, tablefmt="fancy_grid"))

    def print_table_html(self):
        LOG.info("Printing result table: %s", tabulate(self.data, self.headers, tablefmt="html"))
