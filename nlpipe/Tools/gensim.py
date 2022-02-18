"""
wrapper around Gensim
Gensim is designed to process raw, unstructured digital texts (“plain text”) using unsupervised machine learning
algorithms.
The algorithms in Gensim, such as Word2Vec, FastText, Latent Semantic Indexing (LSI, LSA, LsiModel),
Latent Dirichlet Allocation (LDA, LdaModel) etc, automatically discover the semantic structure of documents by examining
statistical co-occurrence patterns within a corpus of training documents. These algorithms are unsupervised, which means
no human input is necessary – you only need a corpus of plain text documents.
"""

from nlpipe.Tools.toolsInterface import Tool
from gensim.models import LdaMulticore, TfidfModel, CoherenceModel, Word2Vec, FastText, Doc2Vec
from gensim.corpora import Dictionary  # create the dictionary/vocabulary from the data
from gensim.models.phrases import Phrases
import multiprocessing  # to speed things up by parallelize
import json
import logging
import csv
from io import StringIO
import numpy as np
import re


class Gensim(Tool):
    name = "gensim"
    args = {}

    def check_status(self):
        pass

    def set_args(self, additional_arguments):
        """
        Set the required arguments for gensim
        """
        self.args = {
            'min_count': additional_arguments.min_count,
            'threshold': additional_arguments.threshold,
            'no_below': additional_arguments.no_below,
            'no_above': additional_arguments.no_above,
            'dev_size': additional_arguments.devsize,
            'min_num_topics': additional_arguments.min_num_topics,
            'max_num_topics': additional_arguments.max_num_topics,
            'model_scoring': additional_arguments.model_scoring  # others can be "c_v"
        }

    def process(self, text, additional_arguments):
        """
        Process the text document
        """
        if hasattr(additional_arguments, "embedding_method"):  # returns embeddings for each token
            model = get_word_embeddings(text, method=additional_arguments.embedding_method)
            return generate_csv_format(np.column_stack((model.wv.vectors, model.wv.index_to_key)))
        else:  # topic modelling
            self.set_args(additional_arguments)
            return _call_gensim(text, self.args)

    def convert(self, doc_id, result, return_format):
        if return_format == "json":
            return json.dumps({"doc_id": doc_id, "status": "OK", "result": result})
        super().convert(result, return_format)


def _call_gensim(text, args):
    """
    Call gensim on the text and return list of topics and 10 frequent words for each
    """
    logging.debug("Generating tokens from the cleaned text")
    if isinstance(text, list):  # if case of corpora
        instances = [t.split() for t in text]
    elif isinstance(text, str):  # in case of a body of text
        instances = [text.split()]

    logging.debug("Generating collocations from the tokens, "
                  "with min_count={min_count} and "
                  "threshold={threshold}".format(min_count=args['min_count'], threshold=args['threshold']))
    phrases = Phrases(instances,  # find phrases
                      min_count=args["min_count"],
                      threshold=args["threshold"])
    instances_colloc = phrases[instances]  # create useful colloc from the text

    logging.debug("Creating a dictionary from the collocations, and getting rid of rare of infrequent words")
    dictionary = Dictionary(instances_colloc)
    # dictionary.filter_extremes(args["no_below"], args["no_above"])  # remove words, not useful for small text

    logging.debug("Translating corpus to IDs, and creating a if-idf tranformation")
    ldacorpus = [dictionary.doc2bow(text) for text in instances_colloc]
    tifidmodel = TfidfModel(ldacorpus)
    model_corpus = tifidmodel[ldacorpus]

    logging.debug("Finding the best model (number of topics), given scoring {model_scoring}"
                  .format(model_scoring=args['model_scoring']))
    best_num_topic = find_best_model(model_corpus, args["dev_size"], args["min_num_topics"], args["max_num_topics"],
                                     dictionary, instances_colloc, args["model_scoring"])

    logging.debug("Best number of topics: {best_num_topic}, training lda model ...".format(**locals()))
    best_model = run_lda_model(model_corpus, dictionary, best_num_topic, random_state=42)

    logging.debug("Reformatting the topic outputs, with best number of topics: {best_num_topic}".format(**locals()))
    words_per_topic = reformat_topic_output(best_model, best_num_topic)
    result = generate_csv_format(words_per_topic)

    print(result)

    return result


def find_best_model(model_corpus, dev_size, min_num_topics, max_num_topics, dictionary, instances_colloc,
                    model_scoring):
    coherence_values = []

    for num_topics in range(min_num_topics, max_num_topics):
        model = run_lda_model(data=model_corpus[:dev_size],
                              id_dict=dictionary,
                              num_topics=num_topics)

        score = calc_model_score(model, instances_colloc[:dev_size], dictionary, score=model_scoring)
        coherence_values.append((num_topics, score))

    coherence_values = np.array(coherence_values)
    return coherence_values[np.argmax(coherence_values[:, 1]), 0]


def run_lda_model(data, id_dict, num_topics, random_state=42):
    num_passes = 10
    # chunk_size = len(data) * num_passes / 200

    model = LdaMulticore(num_topics=num_topics,  # number of topics
                         corpus=data,  # what to train on
                         id2word=id_dict,  # mapping from IDs to words
                         workers=min(10, multiprocessing.cpu_count() - 1),  # choose 10 cores, or whatever computer has
                         passes=num_passes,  # make this many passes over data
                         chunksize=10,  # update after this many instances
                         alpha=0.5,
                         random_state=random_state
                         )
    return model


def calc_model_score(model, data, dict, score):
    coherencemodel_umass = CoherenceModel(model=model,
                                          texts=data,
                                          dictionary=dict,
                                          coherence=score)
    return coherencemodel_umass.get_coherence()


def reformat_topic_output(model, num_topics):
    # get the topic descriptions
    topic_sep = re.compile("0\.[0-9]{3}\*")  # getting rid of useless formatting
    # extract a list of tuples with topic number and descriptors from the model
    model_topics = [(topic_no, re.sub(topic_sep, '', model_topic).split(' + ')) for topic_no, model_topic in
                    model.print_topics(num_topics=num_topics, num_words=5)]

    descriptors = []
    for i, m in model_topics:
        descriptors.append(", ".join(m[:5]).replace('"', ''))

    return descriptors


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


Gensim.register()  # register the tool in known_tools
