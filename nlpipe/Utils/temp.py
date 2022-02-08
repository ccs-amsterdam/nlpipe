from nlpipe.Tools.toolsInterface import Tool
from gensim.models import LdaMulticore, TfidfModel, CoherenceModel, Word2Vec, FastText, Doc2Vec
from gensim.corpora import Dictionary  # create the dictionary/vocabulary from the data
from gensim.models.phrases import Phrases
from gensim.utils import tokenize
import multiprocessing  # to speed things up by parallelizing
import json
import logging
import csv
from io import StringIO
import numpy as np
import re


def generate_csv_format(res):
    s = StringIO()
    w = csv.writer(s)
    logging.debug("writing values")
    w.writerow(res)

    return s.getvalue()


def get_word_embeddings(text, method):
    tokens = [text.split(" ")]
    if method == "Word2vec":
        return (Word2Vec(tokens, vector_size=300, window=5, min_count=1, workers=-1, epochs=10))

    elif method == "FastText":
        model = FastText(vector_size=300, window=5, min_count=1)
        model.build_vocab(corpus_iterable=tokens)
        model.train(corpus_iterable=tokens, total_examples=len(tokens), epochs=10)
        return model
    
    
if __name__ == '__main__':
    model = get_word_embeddings("this is a test", "Word2vec")
    print(generate_csv_format(np.column_stack((model.wv.vectors, model.wv.index_to_key))))