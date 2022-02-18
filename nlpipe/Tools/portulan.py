"""
wrapper for portulan (from CLARIN).
I couldn't find a local implementation, so you need to get a key: https://portulanclarin.net/workbench/lx-depparser/
PORTULAN CLARIN Research Infrastructure for the Science and Technology of Language,
belonging to the Portuguese National Roadmap of Research Infrastructures of Strategic Relevance,
and part of the international research infrastructure CLARIN ERIC.

"""

from nlpipe.Tools.toolsInterface import Tool
import json
import logging
import csv
import requests
from io import StringIO


class Portulan(Tool):
    name = "portulan"
    url = "https://portulanclarin.net/workbench/lx-depparser/api/"

    def check_portulan(self, key):
        """
        Check if the portulan service is running
        :param key: API key (mandatory)
        """
        request_data = {
            'method': 'key_status',
            'jsonrpc': '2.0',
            'id': 0,
            'params': {
                'key': key,
            },
        }
        request = requests.post(self.url, json=request_data)
        response_data = request.json()
        if "error" in response_data:
            print("Error:", response_data["error"])
        else:
            print("Key status:")
            print(json.dumps(response_data["result"], indent=4))

    def check_status(self):
        pass

    def process(self, text, additional_arguments):
        """
        Process the text document. Called by worker
        :param text: text to be processed
        :param additional_arguments: additional arguments for the portulan
        """
        if hasattr(additional_arguments, "portulan_key"):  # API key
            if hasattr(additional_arguments, "portulan_tagset"):  # tagset (what to be processed in text)
                return _call_portulan(url=self.url, text=text, key=additional_arguments.portulan_key,
                                      tagset=additional_arguments.portulan_tagset)
            else:
                return _call_portulan(url=self.url, text=text, key=additional_arguments.portulan_key, tagset="UD")
        else:
            logging.debug("No key was given for portulan")
            return "KEY required for portulan"

    def convert(self, doc_id, result, return_format):
        if return_format == "json":
            return json.dumps({"doc_id": doc_id, "status": "OK", "result": result})
        super().convert(result, return_format)


def _call_portulan(url, text, key, tagset, format='json'):
    """
    Call portulan on the text and return
    """
    request_data = {
        'method': 'parse',
        'jsonrpc': '2.0',
        'id': 0,
        'params': {
            'text': text,
            'tagset': tagset,
            'format': format,
            'key': key,
        },
    }

    request = requests.post(url, json=request_data)
    response_data = request.json()
    if "error" in response_data:
        logging.error("Error in getting the results from portulan")
        return None
    else:
        result = response_data["result"]
        return generate_csv_format(result)


def generate_csv_format(doc):
    s = StringIO()
    w = csv.writer(s)

    logging.debug("writing values")
    for _, paragraph in enumerate(doc, start=1):  # enumerate paragraphs in text, starting at 1
        for _, sentence in enumerate(paragraph, start=1):  # enumerate sentences in paragraph, starting at 1
            for _, token in enumerate(sentence, start=1):  # enumerate tokens in sentence, starting at 1
                w.writerow(token.keys())
                w.writerow(token.values())
    return s.getvalue()


Portulan.register()  # register the tool in known_tools


