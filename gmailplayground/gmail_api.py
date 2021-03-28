import logging
from dataclasses import dataclass
import datetime
from enum import Enum
from typing import List, Dict, Any

from googleapiclient.discovery import build
from pythoncommons.google.google_auth import GoogleApiAuthorizer

from utils import Decoder

LOG = logging.getLogger(__name__)

class HeaderField(Enum):
    NAME = "name"
    VALUE = "value"


class ListQueryParam(Enum):
    QUERY = "q"
    USER_ID = "userId"

class GetAttachmentParam(Enum):
    USER_ID = "userId"
    MESSAGE_ID = "messageId"
    ATTACHMENT_ID = "id"


class ThreadsResponseField(Enum):
    THREADS = "threads"


class MessagePartBodyField(Enum):
    SIZE = "size"
    DATA = "data"
    ATTACHMENT_ID = "attachmentId"


class MessagePartField(Enum):
    PART_ID = "partId"
    MIME_TYPE = "mimeType"
    HEADERS = "headers"
    BODY = "body"
    PARTS = "parts"


class MessageField(Enum):
    ID = "id"
    THREAD_ID = "threadId"
    SNIPPET = "snippet"
    DATE = "internalDate"
    PAYLOAD = "payload"


class ThreadField(Enum):
    ID = "id"
    MESSAGES = "messages"
    SNIPPET = "snippet"


class ApiItemType(Enum):
    THREAD = "thread"
    MESSAGE = "message"

@dataclass
class MessagePartBody:
    data: str
    size: str
    attachmentId: str
    encoding_error: bool = False

@dataclass
class MessagePartBodyWithMissingBodyData:
    message_id: str
    attachment_id: str
    message_part_body: MessagePartBody

@dataclass
class Header:
    name: str
    value: str


@dataclass
class MessagePart:
    id: str
    mimeType: str
    headers: List[Header]
    body: MessagePartBody
    parts: List[Any] # Cannot refer to MessagePart :(


@dataclass
class Message:
    id: str
    threadId: str
    date: datetime.datetime
    snippet: str
    payload: MessagePart


@dataclass
class Thread:
    id: str
    subject: str
    messages: List[Message]


class Progress:
    def __init__(self, item_type: ApiItemType):
        self.requests_count = 0
        self.all_items_count = 0
        self.processed_items = 0
        self.new_items_with_last_request = -1
        self.item_type = item_type

    def _print_status(self):
        LOG.info(f"[Request #: {self.requests_count}] Received {self.new_items_with_last_request} more {self.item_type.value}s")

    def incr_requests(self):
        self.requests_count += 1

    def register_new_items(self, number_of_new_items: int, print_status=True):
        self.all_items_count += number_of_new_items
        self.new_items_with_last_request = number_of_new_items
        if print_status:
            self._print_status()

    def incr_processed_items(self):
        self.processed_items += 1

    def print_processing_items(self):
        LOG.debug(f"Processing {self.item_type.value}s: {self.processed_items} / {self.all_items_count}")


class GmailWrapper:
    USERID_ME = 'me'
    DEFAULT_API_FIELDS = {ListQueryParam.USER_ID.value: USERID_ME}

    def __init__(self, authorizer: GoogleApiAuthorizer, api_version: str = None):
        self.creds = authorizer.authorize()
        if not api_version:
            api_version = authorizer.service_type.default_api_version
        self.service = build(authorizer.service_type.service_name, api_version, credentials=self.creds)
        self.users_svc = self.service.users()
        self.messages_svc = self.users_svc.messages()
        self.threads_svc = self.users_svc.threads()
        self.attachments_svc = self.messages_svc.attachments()
        self.message_part_bodies_without_body: List[MessagePartBodyWithMissingBodyData] = []

    def query_threads_with_paging(self, query: str = None) -> List[Thread]:
        kwargs = self._get_new_kwargs()
        if query:
            kwargs["q"] = query
        request = self.threads_svc.list(**kwargs)

        threads: List[Thread] = []
        progress = Progress(ApiItemType.THREAD)
        self.message_part_bodies_without_body.clear()
        while request is not None:
            response = request.execute()
            if response:
                progress.incr_requests()
                list_of_threads = response.get(ThreadsResponseField.THREADS.value, [])
                progress.register_new_items(len(list_of_threads), print_status=True)

                for idx, thread in enumerate(list_of_threads):
                    progress.incr_processed_items()
                    progress.print_processing_items()
                    thread_data = self._query_thread_data(thread)
                    messages_in_thread = self._get_field(thread_data, ThreadField.MESSAGES)
                    message_objs: List[Message] = [self.parse_api_message(message) for message in messages_in_thread]
                    subject = self._parse_subject_of_message(message_objs[0])
                    thread_obj = Thread(self._get_field(thread_data, ThreadField.ID), subject, message_objs)
                    threads.append(thread_obj)
            request = self.threads_svc.list_next(request, response)

        # TODO error log all messages that had base64 encoding errors
        self._query_attachments_for_missing_message_part_body()
        return threads

    def _query_attachments_for_missing_message_part_body(self):
        # Fix MessagePartBody object that has attachmentId only
        # Quoting from API doc for Field 'attachmentId':
        # When present, contains the ID of an external attachment that can be retrieved in a separate messages.attachments.get request.
        # When not present, the entire content of the message part body is contained in the data field.
        for mpb in self.message_part_bodies_without_body:
            if not mpb.message_id or not mpb.attachment_id:
                LOG.error("Both message_id and attacment_id has to be set in order to query attachment details."
                          f"Object was: {mpb}")
                continue
            self._query_attachment(mpb.message_id, mpb.attachment_id)

    def parse_api_message(self, message: Dict):
        message_part = self._get_field(message, MessageField.PAYLOAD)
        message_id: str = self._get_field(message, MessageField.ID)
        message_part_obj: MessagePart = self.parse_message_part(message_part, message_id)
        return Message(
            message_id,
            self._get_field(message, MessageField.THREAD_ID),
            datetime.datetime.fromtimestamp(int(self._get_field(message, MessageField.DATE)) / 1000),
            self._get_field(message, MessageField.SNIPPET),
            message_part_obj
        )

    def parse_message_part(self, message_part, message_id: str) -> MessagePart:
        message_parts = self._get_field(message_part, MessagePartField.PARTS, [])
        headers = self._parse_headers(message_part)
        message_part_obj: MessagePart = MessagePart(
            self._get_field(message_part, MessagePartField.PART_ID),
            self._get_field(message_part, MessagePartField.MIME_TYPE),
            headers,
            self._parse_message_part_body_obj(self._get_field(message_part, MessagePartField.BODY), message_id),
            [self.parse_message_part(part, message_id) for part in message_parts],
        )
        return message_part_obj

    def _parse_headers(self, message_part):
        headers_list: List[Dict[str, str]] = self._get_field(message_part, MessagePartField.HEADERS)
        headers: List[Header] = []
        for header_dict in headers_list:
             headers.append(Header(self._get_field(header_dict, HeaderField.NAME),
                                   self._get_field(header_dict, HeaderField.VALUE)))
        return headers

    def _parse_message_part_body_obj(self, messagepart_body, message_id: str):
        encoding_error = False
        messagepart_body_data = self._get_field(messagepart_body, MessagePartBodyField.DATA)
        try:
            decoded_body_str = Decoder.decode_base64(messagepart_body_data) if messagepart_body_data else ""
        except:
            LOG.exception(f"Failed to parse base64 encoded data for message with id: {message_id}."
                          f"Storing original body data to object and storing original API object as well.")
            decoded_body_str = messagepart_body_data
            encoding_error = True

        kwargs = {"encoding_error": False}
        if encoding_error:
            kwargs["encoding_error"] = True
        message_part_body_obj = MessagePartBody(decoded_body_str,
                                                self._get_field(messagepart_body, MessagePartBodyField.SIZE),
                                                self._get_field(messagepart_body, MessagePartBodyField.ATTACHMENT_ID),
                                                **kwargs)
        if not decoded_body_str:
            self.message_part_bodies_without_body.append(
                MessagePartBodyWithMissingBodyData(
                    message_id, message_part_body_obj.attachmentId, message_part_body_obj))
        return message_part_body_obj

    def _query_thread_data(self, thread):
        kwargs = self._get_new_kwargs()
        kwargs[ThreadField.ID.value] = self._get_field(thread, ThreadField.ID)
        tdata = self.threads_svc.get(**kwargs).execute()
        return tdata

    def _query_attachment(self, message_id: str, attachment_id: str):
        kwargs = self._get_new_kwargs()
        kwargs[GetAttachmentParam.MESSAGE_ID.value] = message_id
        kwargs[GetAttachmentParam.ATTACHMENT_ID.value] = attachment_id
        attachment_data = self.attachments_svc.get(**kwargs).execute()
        return attachment_data

    def _parse_subject_of_message(self, message: Message):
        for header in message.payload.headers:
            if header.name == 'Subject':
                return header.value
        return None

    def _get_field(self, gmail_api_obj: Dict, field, default_val=None):
        if isinstance(field, Enum):
            if field.value in gmail_api_obj:
                ret = gmail_api_obj[field.value]
            else:
                ret = default_val
            if not ret:
                ret = default_val
            return ret

    @staticmethod
    def _get_new_kwargs():
        kwargs = {}
        kwargs.update(GmailWrapper.DEFAULT_API_FIELDS)
        return kwargs

