import os.path
import errno
import logging, requests
import subprocess
from nlpipe.Tasks.DatabaseTaskManager import Docs
from nlpipe.Utils.utils import STATUS, get_id
from nlpipe.Tools.toolsInterface import get_tool, get_known_tools
from nlpipe.Servers.Storage.StorageInterface import StorageInterface

class AmcatStorage(StorageInterface):
    """
    NLPipe client that relies on Amcat storage (REST API access)
    """

    def __init__(self, server):
        self.server = server  # main directory for storing all files
        for tool in get_known_tools():
            logging.debug("checking amcat server at {result_dir} and tool {tool}".format(**locals()))
            self._check_server(tool.name)  # makes a directory for each tool

    def _check_server(self, tool):
        requests.get(self.server)