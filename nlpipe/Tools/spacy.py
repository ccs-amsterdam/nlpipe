"""
spaCy wrapper
spaCy is a free, open-source library for advanced Natural Language Processing (NLP) in Python.
If you’re working with a lot of text, you’ll eventually want to know more about it.
For example, what’s it about? What do the words mean in context?
Who is doing what to whom? What companies and products are mentioned? Which texts are similar to each other?
"""

from nlpipe.Tools.toolsInterface import Tool
import spacy
import json, sys, subprocess, logging, csv
from io import StringIO


class SpaCy(Tool):
    name = "spacy"

    def check_status(self):
        pass

    def process(self, text, additional_arguments):
        """
        Process the text document. Called by worker
        :param text: text to be processed
        :param additional_arguments: additional arguments for the spacy model
        :param additional_arguments.language_model: language model used for spacy
        :param additional_arguments.field: in case there is a specific filed to be returned
        :return: processed text
        """
        lm = additional_arguments.language_model or 'en_core_web_sm'
        return _call_spacy(text=text, lm=lm, field=additional_arguments.field)

    def convert(self, doc_id, result, return_format):
        """
        Convert the processed document to the requested return format
        """
        if return_format == "json":
            return json.dumps({"doc_id": doc_id, "status": "OK", "result": result})
        super().convert(result, return_format)


def _call_spacy(text, lm, field=None):
    """
    Call spacy on the text and return annotations
    :param text: text to be processed
    :param lm: language model
    :param field: specific field to be returned
    """

    logging.debug("downloading and loading the language model {lm}".format(**locals()))
    check_language_model(lm)
    nlp = spacy.load(lm)

    logging.debug("Creating spacy_udpipe object")
    doc = nlp(text)

    return generate_cvs_format(doc, field)  # generate csv format


def check_language_model(lm):
    """
    Check (and if needed download) the language model
    """
    if not spacy.util.is_package(lm):
        subprocess.check_call([sys.executable, "-m", "spacy", "download", lm])


def generate_cvs_format(doc, field=None):
    s = StringIO()
    w = csv.writer(s)

    logging.debug("writing values")
    if field is not None:
        w.writerow([field])
        for token in doc:
            w.writerow([token.__getattribute__(field)])
    else:
        w.writerow(["text", "lang_", "left_edge", "right_edge", "ent_type_", "lemma_", "morph", "pos_", "dep_"])
        for token in doc:
            w.writerow([token.text, token.lang_, token.left_edge, token.right_edge, token.ent_type_,
                        token.lemma_, token.morph, token.pos_, token.dep_])

    return s.getvalue()


SpaCy.register()  # register the tool in known_tools
