import base64
import logging

LOG = logging.getLogger(__name__)


class Decoder:
    @staticmethod
    def decode_base64(encoded):
        decoded_data = base64.b64decode(encoded)
        return str(decoded_data)