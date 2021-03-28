#!/usr/bin/python

import argparse
import datetime
import logging
import os
import sys
import time
from dataclasses import field, dataclass
from enum import Enum
from logging.handlers import TimedRotatingFileHandler
from os.path import expanduser
from typing import List

from pythoncommons.file_utils import FileUtils
from pythoncommons.google.common import ServiceType
from pythoncommons.google.google_auth import GoogleApiAuthorizer
from pythoncommons.google.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.string_utils import RegexUtils

from gmail_api import GmailWrapper, GmailThreads

LOG = logging.getLogger(__name__)
PROJECT_NAME = "gmail_api_playground"
__author__ = 'Szilard Nemeth'


class OperationMode(Enum):
    GSHEET = "GSHEET"
    PRINT = "PRINT"


class Setup:
    @staticmethod
    def init_logger(log_dir, console_debug=False):
        # get root logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # create file handler which logs even debug messages
        prefix = f"{PROJECT_NAME}-"
        logfilename = datetime.datetime.now().strftime(prefix + "%Y_%m_%d_%H%M%S.log")

        log_file = FileUtils.join_path(log_dir, logfilename)
        fh = TimedRotatingFileHandler(log_file, when='midnight')
        fh.suffix = "%Y_%m_%d.log"
        fh.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(logging.INFO)
        if console_debug:
            ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

    @staticmethod
    def parse_args():
        """This function parses and return arguments passed in"""

        parser = argparse.ArgumentParser()

        parser.add_argument('-v', '--verbose', action='store_true',
                            dest='verbose', default=None, required=False,
                            help='More verbose log')

        exclusive_group = parser.add_mutually_exclusive_group(required=True)
        exclusive_group.add_argument('-p', '--print', action='store_true', dest='do_print',
                                     help='Print results to console',
                                     required=False)
        exclusive_group.add_argument('-g', '--gsheet', action='store_true',
                                     dest='gsheet', default=False,
                                     required=False,
                                     help='Export values to Google sheet. '
                                          'Additional gsheet arguments need to be specified!')

        # Arguments for Google sheet integration
        gsheet_group = parser.add_argument_group('google-sheet', "Arguments for Google sheet integration")

        gsheet_group.add_argument('--gsheet-client-secret',
                                  dest='gsheet_client_secret', required=False,
                                  help='Client credentials for accessing Google Sheet API')

        gsheet_group.add_argument('--gsheet-spreadsheet',
                                  dest='gsheet_spreadsheet', required=False,
                                  help='Name of the GSheet spreadsheet')

        gsheet_group.add_argument('--gsheet-worksheet',
                                  dest='gsheet_worksheet', required=False,
                                  help='Name of the worksheet in the GSheet spreadsheet')

        args = parser.parse_args()
        print("Args: " + str(args))

        # TODO check existence + readability of secret file!!
        if args.gsheet and (args.gsheet_client_secret is None or
                            args.gsheet_spreadsheet is None or
                            args.gsheet_worksheet is None):
            parser.error("--gsheet requires --gsheet-client-secret, --gsheet-spreadsheet and --gsheet-worksheet.")

        if args.do_print:
            print(f"Using operation mode: {OperationMode.PRINT.value}")
            args.operation_mode = OperationMode.PRINT
        elif args.gsheet:
            print(f"Using operation mode: OperationMode.GSHEET.value")
            args.operation_mode = OperationMode.GSHEET
            args.gsheet_options = GSheetOptions(args.gsheet_client_secret,
                                                args.gsheet_spreadsheet,
                                                args.gsheet_worksheet)
        else:
            print("Unknown operation mode!")

        return args


@dataclass
class MatchedLinesFromMessage:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime
    lines: List[str] = field(default_factory=list)


class GmailPlayground:
    def __init__(self, args):
        self.setup_dirs()
        self.operation_mode = args.operation_mode
        self.validate_operation_mode()

        if self.operation_mode == OperationMode.GSHEET:
            self.gsheet_wrapper = GSheetWrapper(args.gsheet_options)

        self.authorizer = GoogleApiAuthorizer(ServiceType.GMAIL)
        self.gmail_wrapper = GmailWrapper(self.authorizer)
        self.headers = ['test1', 'test2']
        self.data = None

    def validate_operation_mode(self):
        if self.operation_mode == OperationMode.PRINT:
            LOG.info("Using operation mode: %s", OperationMode.PRINT)
        elif self.operation_mode == OperationMode.GSHEET:
            LOG.info("Using operation mode: %s", OperationMode.GSHEET)
        else:
            raise ValueError("Unknown state! Operation mode should be either "
                             "{} or {} but it is {}"
                             .format(OperationMode.PRINT,
                                     OperationMode.GSHEET,
                                     self.operation_mode))

    def setup_dirs(self):
        home = expanduser("~")
        self.project_out_root = os.path.join(home, PROJECT_NAME)
        self.log_dir = os.path.join(self.project_out_root, 'logs')
        FileUtils.ensure_dir_created(self.project_out_root)
        FileUtils.ensure_dir_created(self.log_dir)

    def start(self):
        query = "subject:\"YARN Daily unit test report\""
        limit = 3

        # TODO Add these to postprocess config object (including mimetype filtering)
        line_sep = "\\r\\n"
        regex = ".*org\\.apache\\.hadoop.*"
        skip_lines_starting_with = ["Failed testcases:", "FILTER:"]
        matched_lines: List[MatchedLinesFromMessage] = []
        # TODO this produced many errors: Uncomment & try again
        # query = "YARN Daily branch diff report"

        threads: GmailThreads = self.gmail_wrapper.query_threads_with_paging(query=query, limit=limit)
        # TODO write a generator function to GmailThreads that generates List[GmailMessageBodyPart]
        for message in threads.messages:
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body.split(line_sep)
                matched_lines_of_msg: List[str] = []
                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives
                    #  a compiled pattern
                    if not self._check_if_line_is_valid(line, skip_lines_starting_with):
                        LOG.warning(f"Skipping line: {line}")
                        continue
                    if RegexUtils.ensure_matches_pattern(line, regex):
                        LOG.debug(f"[PATTERN: {regex}] Matched line: {line}")
                        matched_lines_of_msg.append(line)

                matched_lines.append(MatchedLinesFromMessage(message.msg_id,
                                                             message.thread_id,
                                                             message.subject,
                                                             message.date,
                                                             matched_lines_of_msg))
        LOG.info("Matched lines: " + str(matched_lines))
        self.process_data()
        self.print_results_table()
        if gmail_playground.operation_mode == OperationMode.GSHEET:
            LOG.info("Updating Google sheet with data...")
            self.update_gsheet()

    def process_data(self):
        pass
        # TODO save & process data
        #  TWO SHEETS
        #  1. Raw data: Non-unique testcase lines
        #  Headers: Testcase, message_id, thread_id, mail subject, datetime, Date (day only)
        #  2. Summary data: Non-Unique testcase lines
        #  Headers: Testcase, Date (day only), frequency of failure
        # self.data: List[List[str]] = DataConverter.convert_data_to_rows(messages_list, truncate=truncate)
        # self.data = threads_list

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

    def print_results_table(self):
        if not self.data:
            raise ValueError("Data is not yet set, please call start method first!")
        # result_printer = ResultPrinter(self.data, self.headers)
        # result_printer.print_table()

    def update_gsheet(self):
        if not self.data:
            raise ValueError("Data is not yet set, please call start method first!")
        self.gsheet_wrapper.write_data(self.headers, self.data)


if __name__ == '__main__':
    start_time = time.time()

    # Parse args
    args = Setup.parse_args()
    gmail_playground = GmailPlayground(args)

    # Initialize logging
    verbose = True if args.verbose else False
    Setup.init_logger(gmail_playground.log_dir, console_debug=verbose)

    gmail_playground.start()
    end_time = time.time()
    LOG.info("Execution of script took %d seconds", end_time - start_time)
