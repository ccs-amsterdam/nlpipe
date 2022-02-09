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
        return _call_spacy(text, lm=additional_arguments.language_model)

    def convert(self, doc_id, result, return_format):
        if return_format == "json":
            return json.dumps({"doc_id": doc_id, "status": "OK", "result": result})
        super().convert(result, return_format)


def _call_spacy(text, lm):
    """
    Call spacy on the text and return annotations
    """

    logging.debug("downloading and loading the language model {lm}".format(**locals()))
    check_language_model(lm)
    nlp = spacy.load(lm)

    logging.debug("Creating spacy_udpipe object")
    doc = nlp(text)

    s = StringIO()
    w = csv.writer(s)
    w.writerow(["text", "orth", "lemma", "pos", "dep", "tag"])
    logging.debug("writing values")
    for token in doc:
        w.writerow([token.text, token.lang_, token.left_edge, token.right_edge, token.ent_type_,
                    token.lemma_, token.morph, token.pos_, token.dep_])

    return s.getvalue()


def check_language_model(lm):
    if not spacy.util.is_package(lm):
        subprocess.check_call([sys.executable, "-m", "spacy", "download", lm])


SpaCy.register()  # register the tool in known_tools
