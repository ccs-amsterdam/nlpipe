from nlpipe.Tools.toolsInterface import Tool
import spacy
import json, sys, subprocess, logging, csv
from io import StringIO


def _call_spacy(text, lm, field=None):
    """
    Call spacy on the text and return annotations
    """

    logging.debug("downloading and loading the language model {lm}".format(**locals()))
    check_language_model(lm)
    nlp = spacy.load(lm)

    logging.debug("Creating spacy_udpipe object")
    doc = nlp(text)

    return generate_cvs_format(doc, field)


def check_language_model(lm):
    if not spacy.util.is_package(lm):
        subprocess.check_call([sys.executable, "-m", "spacy", "download", lm])


def generate_cvs_format(doc, field=None):
    s = StringIO()
    w = csv.writer(s)

    logging.info("writing values")
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



if __name__ == '__main__':
    print(_call_spacy("this is a text", "en_core_web_sm", "pos_"))
    print(_call_spacy("this is a text", "en_core_web_sm"))
