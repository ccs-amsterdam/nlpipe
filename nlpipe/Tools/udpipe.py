"""
spaCy wrapper around udpipe
The data preparation part of any Natural Language Processing flow consists of a number of important steps: 
Tokenization (1), Parts of Speech tagging (2), Lemmatization (3) and Dependency Parsing (4). 
This package allows you to do out-of-the-box annotation of these 4 steps and also allows you to train your own annotator models

"""

from nlpipe.Tools.toolsInterface import Tool
import spacy_udpipe
import json
import logging
import csv
from io import StringIO


class Udpipe(Tool):
    name = "udpipe"

    def check_status(self):
        pass

    def process(self, text, additional_arguments):
        """
        Process the text document. Called by worker
        :param text: text to be processed
        :param additional_arguments: additional arguments for the udpipe model
        """
        lm = additional_arguments.language_model or 'en'
        return _call_udpipe(text, lm=lm)

    def convert(self, doc_id, result, return_format):
        """
        Convert the processed document to the requested return format
        """
        if return_format == "json":
            return json.dumps({"doc_id": doc_id, "status": "OK", "result": result})
        super().convert(result, return_format)


def _call_udpipe(text, lm):
    """
    Call udpipe on the text and return (text, orth_, lemma_, pos_, dep_, tag_) tuples
    :param text: text to be processed
    :param lm: udpipe language model
    :return: token.text, orth_, lemma_, pos_, dep_, tag_ for each token tuples in csv format
    """

    logging.debug("downloading and loading the language model {lm}".format(**locals()))
    spacy_udpipe.download(lm)  # download (if needed) the language model
    nlp = spacy_udpipe.load(lm)  # load the language model

    logging.debug("Creating spacy_udpipe object")
    doc = nlp(text)  # create a spacy_udpipe.nlp object

    s = StringIO()
    w = csv.writer(s)
    w.writerow(["text", "orth", "lemma", "pos", "dep", "tag"])
    logging.debug("writing values")
    for token in doc:
        w.writerow([token.text, token.orth_, token.lemma_, token.pos_, token.dep_, token.tag_])

    return s.getvalue()


Udpipe.register()  # register the tool in known_tools
