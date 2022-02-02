"""
Wrapper around the RUG Alpino Dependency parser
The module expects either ALPINO_HOME to point at the alpino installation dir
or an alpino server to be running at ALPINO_SERVER (default: localhost:5002)

You can use the following command to get the server running: (see github.com/vanatteveldt/alpino-server)
docker run -dp 5002:5002 vanatteveldt/alpino-server

If running alpino locally, note that the module needs the dependencies end_hook, which seems to be missing in
some builds. See: http://www.let.rug.nl/vannoord/alp/Alpino
"""
import csv
import json
import logging
import os
import subprocess
import requests
import tempfile
from io import StringIO
from nlpipe.Tools.toolsInterface import Tool

log = logging.getLogger(__name__)

CMD_PARSE = ["bin/Alpino", "end_hook=dependencies", "-parse"]
CMD_TOKENIZE = ["Tokenization/tok"]


class AlpinoParser(Tool):
    name = "alpino"

    def check_status(self):  # check if the alpino server is running
        if 'ALPINO_HOME' in os.environ:
            alpino_home = os.environ['ALPINO_HOME']
            if not os.path.exists(alpino_home):
                raise Exception("Alpino not found at ALPINO_HOME={alpino_home}".format(**locals()))
        else:
            alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')  # server runs on port 5002
            r = requests.get(alpino_server)  # check on the server
            if r.status_code != 200:
                raise Exception("No server found at {alpino_server} and ALPINO_HOME not set".format(**locals()))

    def process(self, text, **kwargs):  # process the test using alpino
        if 'ALPINO_HOME' in os.environ:  # run using command line (not using server API)
            tokens = tokenize(text)  # tokenize the text
            return parse_raw(tokens)
        else:
            alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')
            url = "{alpino_server}/parse".format(**locals())
            body = {"text": text, "output": "dependencies"}
            r = requests.post(url, json=body)
            if r.status_code != 200:
                raise Exception("Error calling Alpino at {alpino_server}: {r.status_code}:\n{r.content!r}"
                                .format(**locals()))
            return r.text

    def convert(self, doc_id, result, return_format):
        """
        convert the text to an indicated return_format
        :param doc_id: id of the document
        :result: text/output to convert
        :return_format: e.g., csv
        :return: converted format
        """
        assert return_format in ["csv"]
        s = StringIO()
        w = csv.writer(s)  # write in csv
        w.writerow(["doc", "doc_id", "sentence", "offset", "word", "lemma", "pos", "rel", "parent"])  # for each row
        for line in interpret_parse(result):  # read line by line and format the results
            w.writerow((doc_id,) + line)
        return s.getvalue()


AlpinoParser.register()  # register alpino in the known_tools


def _call_alpino(command, input_text):
    """
    Calls alpino given the command and input text
    """
    alpino_home = os.environ['ALPINO_HOME']
    p = subprocess.Popen(command, shell=False, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         cwd=alpino_home)  # start a subprocess with the command
    out, err = [x.decode("utf-8") for x in p.communicate(input_text.encode("utf-8"))]
    if not out:
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="wb") as f:
            f.write(input_text.encode("utf-8"))
            logging.exception("Error calling Alpino, input file written to {f.name}, command was {command}"
                              .format(**locals()))
        raise Exception("Problem calling {command}, output was empty. Error: {err!r}".format(**locals()))
    return out


def tokenize(text: str) -> str:  # tokenize and replace "|"
    return _call_alpino(CMD_TOKENIZE, text).replace("|", "")


def parse_raw(tokens):  # parse the tokens
    return _call_alpino(CMD_PARSE, tokens)


def get_fields(parse):
    if parse.strip().startswith("{"):
        parse = json.loads(parse)
        for sid in parse:
            for row in parse[sid]['triples']:
                yield row + [sid]
    else:
        for line in parse.split("\n"):
            if line.strip():
                yield line.strip().split("|")


def interpret_parse(parse):
    rels = {}  # child: (rel, parent)
    for line in get_fields(parse):
        assert len(line) == 16
        sid = int(line[-1])
        func, rel = line[7].split("/")
        child = interpret_token(sid, *line[8:15])
        if func == "top":
            parent = None
        else:
            parent = interpret_token(sid, *line[:7])
        rels[child] = (rel, parent)

    # get tokenid for each token, preserving order
    tokens = sorted(rels.keys(), key=lambda token: token[:2])
    tokenids = {token: i for (i, token) in enumerate(tokens)}

    for token in tokens:
        (rel, parent) = rels[token]
        tokenid = tokenids[token]
        parentid = tokenids[parent] if parent is not None else None
        yield (tokenid, ) + token + (rel, parentid)


def interpret_token(sid, lemma, word, begin, _end, major_pos, _pos, full_pos):
    """Convert to raw alpino token into a (word, lemma, begin, pos1) tuple"""
    if major_pos not in POSMAP:
        logging.warn("UNKNOWN POS: {major_pos}".format(**locals()))
    pos1 = POSMAP.get(major_pos, '?')  # simplified POSMAP
    return sid, int(begin), word, lemma, pos1


POSMAP = {"pronoun": 'O', "pron": 'O',
          "verb": 'V',
          "noun": 'N',
          "preposition": 'P', "prep": 'P',
          "determiner": "D",  "det": "D",
          "comparative": "C",  "comp": "C",
          "adverb": "B",
          'adv': 'B',
          "adj": "A",
          "complementizer": "C",
          "punct": ".",
          "conj": "C",
          "vg": 'C', "prefix": 'C',  # not quite sure what vg stands for, sorry
          "tag": "?",
          "particle": "R",  "fixed": 'R',
          "name": "M",
          "part": "R",
          "intensifier": "B",
          "number": "Q", "num": 'Q',
          "cat": "Q",
          "n": "Q",
          "reflexive":  'O',
          "conjunct": 'C',
          "pp": 'P',
          'anders': '?',
          'etc': '?',
          'enumeration': '?',
          'np': 'N',
          'p': 'P',
          'quant': 'Q',
          'sg': '?',
          'zo': '?',
          'max': '?',
          'mogelijk': '?',
          'sbar': '?',
          '--': '?',
          }
