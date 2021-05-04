#!/usr/bin/python

import argparse
import datetime
import logging
import copy
import sys
import time
from dataclasses import field, dataclass
from enum import Enum
from logging.handlers import TimedRotatingFileHandler
from typing import List, Dict

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, GmailThreads
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.result_printer import BasicResultPrinter
from pythoncommons.string_utils import RegexUtils

DEFAULT_LINE_SEP = "\\r\\n"

LOG = logging.getLogger(__name__)
PROJECT_NAME = "gmail_api_playground"
__author__ = 'Szilard Nemeth'

# REQ_LIMIT = 1000
REQ_LIMIT = 1


class OperationMode(Enum):
    GSHEET = "GSHEET"
    PRINT = "PRINT"


class Setup:
    @staticmethod
    def init_logger(console_debug=False):
        # get root logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # create file handler which logs even debug messages
        logfilename = ProjectUtils.get_default_log_file(PROJECT_NAME)
        fh = TimedRotatingFileHandler(logfilename, when='midnight')
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
        ProjectUtils.get_output_basedir(PROJECT_NAME)
        self.operation_mode = args.operation_mode
        self.validate_operation_mode()

        if self.operation_mode == OperationMode.GSHEET:
            self.gsheet_wrapper_normal = GSheetWrapper(args.gsheet_options)
            gsheet_options = copy.copy(args.gsheet_options)
            gsheet_options.worksheet = gsheet_options.worksheet + "_aggregated"
            self.gsheet_wrapper_aggregated = GSheetWrapper(gsheet_options)

        self.authorizer = GoogleApiAuthorizer(ServiceType.GMAIL)
        self.gmail_wrapper = GmailWrapper(self.authorizer)

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

    def start(self):
        # TODO Query mapreduce failures to separate sheet
        # TODO implement caching of emails in json files
        # TODO Split by [] --> Example: org.apache.hadoop.yarn.util.resource.TestResourceCalculator.testDivisionByZeroRatioNumeratorAndDenominatorIsZero[1]
        query = "subject:\"YARN Daily unit test report\""
        # TODO Add these to postprocess config object (including mimetype filtering)
        regex = ".*org\\.apache\\.hadoop\\.yarn.*"
        skip_lines_starting_with = ["Failed testcases:", "FILTER:"]

        # TODO this query below produced some errors: Uncomment & try again
        # query = "YARN Daily branch diff report"
        threads: GmailThreads = self.gmail_wrapper.query_threads_with_paging(query=query, limit=REQ_LIMIT)
        # TODO write a generator function to GmailThreads that generates List[GmailMessageBodyPart]
        raw_data = self.filter_data_by_regex_pattern(threads, regex, skip_lines_starting_with)
        self.process_data(raw_data)

    def filter_data_by_regex_pattern(self, threads, regex, skip_lines_starting_with, line_sep=DEFAULT_LINE_SEP):
        matched_lines: List[MatchedLinesFromMessage] = []
        for message in threads.messages:
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body.split(line_sep)
                matched_lines_of_msg: List[str] = []
                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
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
        LOG.debug(f"[RAW DATA] Matched lines: {matched_lines}")
        return matched_lines

    def process_data(self, raw_data: List[MatchedLinesFromMessage]):
        truncate = self.operation_mode == OperationMode.PRINT
        header = ["Date", "Subject", "Testcase", "Message ID", "Thread ID"]
        converted_data: List[List[str]] = DataConverter.convert_data_to_rows(raw_data, truncate=truncate)
        self.print_results_table(header, converted_data)

        if gmail_playground.operation_mode == OperationMode.GSHEET:
            LOG.info("Updating Google sheet with data...")
            header_aggregated = ["Testcase", "Frequency of failures", "Latest failure"]
            aggregated_data: List[List[str]] = DataConverter.convert_data_to_aggregated_rows(raw_data)
            self.update_gsheet(header, converted_data)
            self.update_gsheet_aggregated(header_aggregated, aggregated_data)

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

    @staticmethod
    def print_results_table(header, data):
        BasicResultPrinter.print_table(data, header)

    def update_gsheet(self, header, data):
        self.gsheet_wrapper_normal.write_data(header, data, clear_range=False)

    def update_gsheet_aggregated(self, header, data):
        self.gsheet_wrapper_aggregated.write_data(header, data, clear_range=False)


class DataConverter:
    SUBJECT_MAX_LENGTH = 50
    LINE_MAX_LENGTH = 80

    @staticmethod
    def convert_data_to_rows(raw_data: List[MatchedLinesFromMessage], truncate: bool = False) -> List[List[str]]:
        converted_data: List[List[str]] = []
        truncate_subject: bool = truncate
        truncate_lines: bool = truncate

        for matched_lines in raw_data:
            for testcase_name in matched_lines.lines:
                subject = matched_lines.subject
                if truncate_subject and len(matched_lines.subject) > DataConverter.SUBJECT_MAX_LENGTH:
                    subject = DataConverter._truncate_str(matched_lines.subject, DataConverter.SUBJECT_MAX_LENGTH, "subject")
                if truncate_lines:
                    testcase_name = DataConverter._truncate_str(testcase_name, DataConverter.LINE_MAX_LENGTH, "testcase")
                row: List[str] = [str(matched_lines.date), subject, testcase_name,
                                  matched_lines.message_id, matched_lines.thread_id]
                converted_data.append(row)
        return converted_data

    @staticmethod
    def convert_data_to_aggregated_rows(raw_data: List[MatchedLinesFromMessage]) -> List[List[str]]:
        failure_freq: Dict[str, int] = {}
        latest_failure: Dict[str, datetime.datetime] = {}
        for matched_lines in raw_data:
            for testcase_name in matched_lines.lines:
                if testcase_name not in failure_freq:
                    failure_freq[testcase_name] = 1
                    latest_failure[testcase_name] = matched_lines.date
                else:
                    failure_freq[testcase_name] = failure_freq[testcase_name] + 1
                    if latest_failure[testcase_name] < matched_lines.date:
                        latest_failure[testcase_name] = matched_lines.date

        converted_data: List[List[str]] = []
        for tc, freq in failure_freq.items():
            last_failed = latest_failure[tc]
            row: List[str] = [tc, freq, str(last_failed)]
            converted_data.append(row)
        return converted_data

    @staticmethod
    def _truncate_str(value: str, max_len: int, field_name: str):
        orig_value = value
        truncated = value[0:max_len] + "..."
        LOG.debug(f"Truncated {field_name}: "
                  f"Original value: '{orig_value}', "
                  f"Original length: {len(orig_value)}, "
                  f"New value (truncated): {truncated}, "
                  f"New length: {max_len}")
        return truncated

    @staticmethod
    def _truncate_date(date):
        original_date = date
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
        truncated = date_obj.strftime("%Y-%m-%d")
        LOG.debug(f"Truncated date. "
                  f"Original value: {original_date},"
                  f"New value (truncated): {truncated}")
        return truncated


if __name__ == '__main__':
    start_time = time.time()

    # Parse args
    args = Setup.parse_args()
    gmail_playground = GmailPlayground(args)

    # Initialize logging
    verbose = True if args.verbose else False
    Setup.init_logger(console_debug=verbose)

    gmail_playground.start()
    end_time = time.time()
    LOG.info("Execution of script took %d seconds", end_time - start_time)
