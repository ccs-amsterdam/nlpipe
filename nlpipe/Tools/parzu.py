"""
arZu is a dependency parser for German. This means that it analyzes the linguistic structure of sentences and,
among other things, identifies the subject and object(s) of a verb.

It is a fork of Gerold Schneider's English Pro3Gres parser. Its architecture is hybrid and consists of both
a hand-written grammar and a statistics module that returns the most likely analysis of a sentence.
The primary difference to the English parser is the German grammar and statistics module. Architecturally,
it is different in that it supports the use of morphological information, and does not use a chunker.
ParZu also has a python wrapper that supports various input/output formats and multiprocessing.

check: https://github.com/rsennrich/ParZu

You can run parzu also using docker: docker run -p 5003:5003 rsennrich/parzu
"""

import json
import os
import requests
from nlpipe.Tools.toolsInterface import Tool


class ParzuClient(Tool):
    name = "parzu"

    def check_status(self):
        parzu_server = os.environ.get('PARZU_SERVER', 'http://localhost:5003')
        r = requests.get(parzu_server)
        if r.status_code != 200:
            raise Exception("No server found at {parzu_server}".format(**locals()))

    def process(self, text, **kwargs):
        parzu_server = os.environ.get('PARZU_SERVER', 'http://localhost:5003')
        url = "{parzu_server}/parse/".format(**locals())
        data = {"text": text}
        r = requests.post(url, data=json.dumps(data))
        r.raise_for_status()
        return r.content.decode("utf-8")

    def convert(self, doc_id, result, return_format):
        assert return_format in ["csv"]
        header = "doc_id, word, lemma, pos, pos2, features, parent, relation, extra1, extra2\n"
        return header + result


ParzuClient.register()
