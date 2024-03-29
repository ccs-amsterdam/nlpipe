import os.path
import errno
import logging
import subprocess
from nlpipe.Tools.toolsInterface import get_tool, get_known_tools
from nlpipe.Utils.utils import STATUS, get_id
from nlpipe.Storage.StorageInterface import StorageInterface


class FileSystemStorage(StorageInterface):
    """
    NLPipe client that relies on direct filesystem access (e.g. on local machine or over NFS)
    """

    def __init__(self, task_manager, result_dir):
        self.TM = task_manager
        self.result_dir = result_dir  # main directory for storing all files
        for tool in get_known_tools():
            logging.debug("checking directory for {result_dir} and tool {tool}".format(**locals()))
            self._check_dirs(tool.name)  # makes a directory for each tool

    def _check_dirs(self, tool: str):
        try:
            os.makedirs(os.path.join(self.result_dir, tool))  # directory name per tool
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def _write(self, tool, doc_id, doc):
        self._check_dirs(tool)  # check if the tool directory exits
        fn = self._filename(tool, doc_id)  # create the file name with only the doc_id (task tracking by another module)
        open(fn, 'w', encoding="UTF-8").write(doc)  # write the document
        return fn

    def _read(self, tool, doc_id):
        fn = self._filename(tool, doc_id)  # file name only based on the tool and doc_id
        return open(fn, encoding="UTF-8").read()  # read the file and return

    def _delete(self, tool, doc_id):
        fn = self._filename(tool, doc_id)  # file name only based on the tool and doc_id
        os.remove(fn)  # delete file

    def _filename(self, tool, doc_id=None):
        tool_directory = os.path.join(self.result_dir, tool)  # separate directory per tool
        if doc_id is None:
            return tool_directory  # if no doc_id is given, return the base directory
        else:
            return os.path.join(tool_directory, str(doc_id))  # if doc_id is given, return full path

    def check(self, tool):  # no idea what this is
        self._check_dirs(tool)
        return tool.check_status()

    def process(self, tool, doc, doc_id=None, task_id=None, doc_status="UNKNOWN", reset_error=False, reset_pending=False):
        """
        Process the task based on the status of the current document.
        - If "UNKNOWN": it stores the doc using the docStorageModule, and changes the status to "PENDING"
        - if "ERROR" or "STARTED" & reset_pending == TRUE: replaces the previous doc and changes the status to "PENDING"
        :param tool: NLP text processing tool (e.g., UPPER_CASE)
        :param doc: document which the NLP task is executed on
        :param task_id: id of the task
        :param doc_id: id of the document
        :param doc_status: status of the document
        :param reset_error: --
        :param reset_pending: --
        :return: doc_id, filename
        """
        if doc_id is None:
            doc_id = get_id(tool, doc)  # generate a doc_id based on md5 hash

        if doc_status == 'PENDING':  # new document
            logging.debug("Assigning doc {doc_id} to {tool}".format(**locals()))
            fn = self._write(tool, doc_id, doc)  # create the file and store the doc

        elif (doc_status == "ERROR" and reset_error) or (doc_status == "STARTED" and reset_pending):
            logging.debug("Re-assigning doc {doc_id} with status {status} to {tool}".format(**locals()))

        else:
            logging.debug("Document {doc_id} had status {}".format(self.status(tool, doc_id), **locals()))

        return doc_id, self._filename(tool, doc_id)

    def result(self, tool, doc_id, return_format=None):
        """
        Returns the results of processing (via the NLP tool) on the document
        :param tool: specific NLP tool (e.g., TEST_UPPER)
        :param doc_id: id of the document
        :param return_format: return format (e.g., json)
        :return: result of processing the document, converted if indicated
        """
        status = self.TM.get_doc_status(doc_id)  # get the current status
        if status == 'DONE':  # if it is done (i.e., the process has finished)
            result = self._read(tool, doc_id)  # read the file
            if return_format is not None:  # for instance can be json
                try:
                    result = get_tool(tool).convert(doc_id, result, return_format)
                except:
                    logging.exception("Error converting document {doc_id} to {return_format}".format(**locals()))
                    raise
            return result
        if status == 'ERROR':
            raise ValueError("Status of {doc_id} is ERROR")
        raise ValueError("Status of {doc_id} is {status}".format(**locals()))

    def get_task(self, tool, doc_id):
        """
        Returns the next document which has the status=="PENDING" for processing by the worker
        :param tool: name of the specific NLP tool for processing
        :param doc_id: id of the document
        :return: filename, and document
        """
        try:
            fn = self._filename(tool, doc_id)  # generates the filename for the document
            if not fn:
                return None, None  # no files to process
            return doc_id, self._read(tool, fn)
        except:
            return None, None

    def store_result(self, tool, doc_id, result):
        """
        Stores the results of applying the tool to the document
        :param tool: name of the tool
        :param doc_id: id of the document
        :param result: processed text
        :return: -
        """
        status = self.TM.get_doc_status(doc_id)  # get the current status
        if status not in ('STARTED', 'DONE', 'ERROR'):  # i don't get this part
            raise ValueError("Cannot store result for task {doc_id} with status {status}".format(**locals()))
        self._write(tool, doc_id, result)  # replace the previous document with the new one

    def store_error(self, tool, doc_id, result):
        """
        Stores the results of applying the tool to the document, in case there was an error
        :param tool: name of the specific NLP tool
        :param doc_id: id of the document
        :param result: result of applying the tool to the document
        :return: -
        """
        status = self.TM.get_doc_status(doc_id)  # get the current status
        if status not in ('STARTED', 'DONE', 'ERROR'):  # i don't get this part
            raise ValueError("Cannot store error for task {doc_id} with status {status}".format(**locals()))
        self._write(tool, doc_id, result) # replace the previous document with the new one

    def statistics(self, module):
        """Get number of docs for each status for this module"""
        for status in STATUS:
            path = self._filename(module, status)
            cmd = "ls {path} | wc -l".format(**locals())
            n = int(subprocess.check_output(cmd, shell=True).decode("utf-8"))
            yield status, n