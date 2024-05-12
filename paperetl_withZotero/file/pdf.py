"""
PDF processing module
"""

from io import StringIO

import requests

from .tei import TEI

from icecream import ic
from .Zotero_extractor import extract_zotero_metadata_to_dictionary


class PDF:
    """
    Methods to transform medical/scientific PDFs into article objects.
    """

    @staticmethod
    def parse(stream, source, path):
        """
        Parses a medical/scientific PDF datastream and returns a processed article.

        Args:
            stream: handle to input data stream
            source: text string describing stream source, can be None

        Returns:
            Article
        """

        # Attempt to convert PDF to TEI XML
        xml = PDF.convert(stream)
        try:
            zotero_metadata = ic(extract_zotero_metadata_to_dictionary(path))
        except IndexError:
            ic('No metadata found')
            zotero_metadata = None

        # Parse and return object
        return TEI.parse(xml, source, zotero_metadata) if xml else None

    @staticmethod
    def convert(stream):
        """
        Converts a medical/scientific article PDF into TEI XML via a GROBID Web Service API call.

        Args:
            stream: handle to input data stream

        Returns:
            TEI XML stream
        """

        # Call GROBID API
        response = requests.post(
            "http://localhost:8070/api/processFulltextDocument", files={"input": stream}
        )

        # Validate request was successful
        if not response.ok:
            print(f"Failed to process file - {response.text}")
            return None

        # Wrap as StringIO
        return StringIO(response.text)
