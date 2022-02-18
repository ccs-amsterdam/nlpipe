import time
import sys
import subprocess
import logging
from typing import Iterable
from multiprocessing import Process
from pydoc import locate
from nlpipe.Clients import client
from nlpipe.Tools.toolsInterface import Tool as Client
from nlpipe.Tools.toolsInterface import get_tool


class Worker(Process):
    """
    Base class for NLP workers.
    Workers check the server periodically for a NLP task, and if it is assigned to them (based on the tool), they
    request the document, process the documents, and post the results back on the server.
    """

    sleep_timeout = 5  # check the tasks every 3 seconds

    def __init__(self, client, tool, args=None, quit=False):
        """
        init function for worker class
        :param client: a Client object to connect to the NLP Server (e.g., HTTP Client)
        :param tool: The module to perform work on
        :param quit: if True, quit if no jobs are found; if False, poll server every second.
        """
        super().__init__()
        self.client = client
        self.tool = tool
        self.arguments = args
        self.quit = quit

    def run(self):
        """
        Check the server every sleep_timeout seconds for a new task assigned to it
        If there are tasks, sends the document to the tool.process() and stores the results
        """
        while True:
            doc_id, doc = self.client.get_task(self.tool.name)
            if doc_id is None:
                if self.quit:
                    logging.info("No jobs for {self.tool.name}, quitting!".format(**locals()))
                    break
                time.sleep(self.sleep_timeout)  # sleep and check again for a new task
                continue
            logging.info("Received task {self.tool.name}/{doc_id} ({n} bytes)".format(n=len(doc), **locals()))
            try:
                result = self.tool.process(doc, additional_arguments=self.arguments)  # process the new task
                self.client.store_result(self.tool.name, doc_id, result)  # store teh results
                logging.debug("Successfully completed task {self.tool.name}/{doc_id} ({n} bytes)"
                              .format(n=len(result), **locals()))
            except Exception as e:
                logging.exception("Exception on parsing {self.tool.name}/{doc_id}"
                                  .format(**locals()))
                try:
                    self.client.store_error(self.tool.name, id, str(e))
                except:
                    logging.exception("Exception on storing error for {self.tool.name}/{doc_id}"
                                      .format(**locals()))


def _import(name):
    result = locate(name)
    if result is None:
        raise ValueError("Cannot import {name!r}".format(**locals()))
    return result


def run_workers(client: Client, tools: Iterable[str], nprocesses: int = 1, quit: bool = False, additional_arguments=None) -> Iterable[Worker]:
    """
    Run the given workers as separate processes
    :param client: a nlpipe.Clients.ClientInterface object
    :param tools: names of the tools (tools name or fully qualified class name)
    :param nprocesses: Number of processes per tool
    :param quit: If True, workers stop when no jobs are present; if False, they poll the server every second
    :param additional_arguments: other arguments passed in
    """
    # import built-in workers
    # import nlpipe.modules
    # create and start workers
    result = []  # don't yield, result can be ignored silently
    for tool_class in tools:
        if "." in tool_class:
            tool = _import(tool_class)()
        else:
            tool = get_tool(tool_class)
        for i in range(1, nprocesses + 1):
            logging.debug("[{i}/{nprocesses}] Starting worker {tool}".format(**locals()))
            if hasattr(additional_arguments, 'lang_model'):
                logging.debug("selected language model: {lang_model}".format(**locals()))
            Worker(client=client, tool=tool, args=additional_arguments, quit=quit).start()
        result.append(tool)

    logging.info("Workers active and waiting for input")
    return result


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("server", help="Server hostname or directory location")
    parser.add_argument("tools", nargs="+", help="Class names of tool(s) to run")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true", default=False)
    parser.add_argument("--processes", "-p", help="Number of processes per worker", type=int, default=1)
    parser.add_argument("--quit", "-q", help="Quit if no jobs are available", action="store_true", default=False)
    parser.add_argument("--token", "-t", help="Provide auth token"
                                              "(default reads ./.nlpipe_token or NLPIPE_TOKEN")

    # for udpipe or spacy
    parser.add_argument("--language_model", "-L", help="(Mandatory for spaCy/udpipe) language model", type=str)
    parser.add_argument("--field", help="(Mandatory for spaCy/udpipe) requested field from parser (e.g.: pos_", type=str)

    # for gensim embedding
    parser.add_argument("--embedding_method", help="(Mandatory for gensim embedding) embedding method", type=str)

    # for gensim topic modelling
    parser.add_argument("--min_count",
                        help="(Optional for gensim topic modelling) min_count for phrases", type=str, default=5)
    parser.add_argument("--threshold",
                        help="(Optional for gensim topic modelling) threshold for phrases", type=str, default=1)
    parser.add_argument("--no_below",
                        help="(Optional for gensim topic modelling) minimum occurrence for rare words",
                        type=str, default=50)
    parser.add_argument("--no_above",
                        help="(Optional for gensim topic modelling) maximum portion for common words",
                        type=str, default=0.3)
    parser.add_argument("--devsize",
                        help="(Optional for gensim topic modelling) number of documents in corpus to use",
                        type=str, default=10000)
    parser.add_argument("--min_num_topics",
                        help="(Optional for gensim topic modelling) minimum number of topics", type=str, default=2)
    parser.add_argument("--max_num_topics",
                        help="(Optional for gensim topic modelling) maximum number of topics", type=str, default=4)
    parser.add_argument("--model_scoring",
                        help="(Optional for gensim topic modelling) scoring topic models", type=str, default="u_mass")

    # for portulan
    parser.add_argument("--portulan_key", help="(Mandatory for portulan) for Portulan: API key",
                        type=str, default=None)
    parser.add_argument("--portulan_tagset", help="(Mandatory for portulan) tagset for Portulan",
                        type=str, default="UD")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')

    client = client.get_client(args.server, token=args.token)

    run_workers(client, args.tools, nprocesses=args.processes, quit=args.quit, additional_arguments=args)
