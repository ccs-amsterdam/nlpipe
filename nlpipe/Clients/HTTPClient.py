import time
import requests
import itertools
from nlpipe.Utils.utils import get_id
from nlpipe.helpers import ERROR_MIME


class Client(object):
    """Abstract class for NLPipe client bindings"""

    def process(self, tool, doc, doc_id=None, reset_error=False, reset_pending=False):
        """Add a document to be processed by tool, returning the task ID
        :param tool: tool name
        :param doc: A document (string)
        :param doc_id: An optional id for the document
        :param reset_error: Re-assign documents that have status 'ERROR'
        :param reset_pending: Re-assign documents that have status 'PENDING'
        :return: task ID
        :rtype: str
        """
        raise NotImplementedError()

    def status(self, tool, doc_id):
        """Get processing status
        :param tool: tool name
        :param doc_id: document ID
        :return: any of 'UNKNOWN', 'PENDING', 'STARTED', 'DONE', 'ERROR'
        """
        raise NotImplementedError()

    def result(self, tool, doc_id, return_format=None):
        """Get processing result, optionally converted to a specified format.
        If the status is ERROR, the result will be raised as an exception
        :param tool: tool name
        :param doc_id: A document (string) or task ID
        :param return_format: (Optional) format to convert to, e.g. 'xml', 'csv', 'json'
        :return: The result of processing (string)
        """
        raise NotImplementedError()

    def process_inline(self, tool, doc, return_format=None, doc_id=None):
        """
        Process the given document, use cached version if possible, wait and return result
        :param tool: tool name
        :param doc: A document (string)
        :param doc_id: id of the document
        :param return_format: optional return format (e.g., 'json')
        :return: The result of processing (string)
        """
        if doc_id is None:
            doc_id = get_id(doc)
        if self.status(tool, doc_id) == 'UNKNOWN':
            self.process(tool, doc, doc_id)
        while True:
            status = self.status(tool, doc_id)
            if status in ('DONE', 'ERROR'):
                return self.result(tool, doc_id, return_format=return_format)
            time.sleep(0.1)

    def get_task(self, tool):
        """
        Get a document to process with the given tool, marking the document as 'in progress'
        :param tool: Name of the tool
        :return: a pair (id, string) for the document to be processed
        """
        raise NotImplementedError()

    def get_tasks(self, tool, n):
        """
        Get multiple documents to process
        :param tool: Name of the tool for processing
        :param n: Number of documents to retrieve
        :return: a sequence of (id, document string) pairs
        """
        for i in range(n):
            yield self.get_task(tool)

    def store_result(self, tool, doc_id, result):
        """
        Store the given result
        :param tool: tool name
        :param doc_id: Document ID
        :param result: Result (string)
        """
        raise NotImplementedError()

    def store_error(self, tool, doc_id, result):
        """
        Store an error
        :param tool: tool name
        :param doc_id: Document ID
        :param result: Result (string) describing the error
        """
        raise NotImplementedError()

    def bulk_status(self, tool, doc_ids):
        """Get processing status of multiple ids
        :param tool: tool name
        :param doc_ids: document IDs
        :return: a dict of {id: status}
        """
        return {id: self.status(tool, idx) for idx in doc_ids}

    def bulk_result(self, tool, doc_ids, return_format=None):
        """Get results for multiple ids
        :param tool: tool name
        :param doc_ids: Task IDs
        :return: a dict of {id: result}
        """
        return {id: self.result(tool, idx, return_format=format) for idx in doc_ids}

    def bulk_process(self, tool, docs, doc_ids=None, **kargs):
        """
        Add multiple documents to the processing queue
        :param tool:  tool name
        :param docs: Documents to process
        :param doc_ids: Optional sequence of explicit IDs corresponding to the documents
        :param kargs: Additional options to pass to process
        :return: a sequence of IDs
        """
        if doc_ids is None:
            doc_ids = itertools.repeat(None)
        return [self.process(tool, doc, doc_id=idx, **kargs) for (doc, idx) in zip(docs, doc_ids)]


class HTTPClient(Client):
    """
    NLPipe client that connects to the REST server
    """

    def __init__(self, server="http://localhost:5000", token=None):
        self.server = server  # server address. default is localhost:5000
        self.token = token  # authentication token

    def request(self, method, url, headers=None, **kwargs):
        """
        Creates, executes, and returns a request object

        :param method: request method (e.g., GET, POST, ...)
        :param url: target URL (endpoint)
        :param headers: additional headers, for instance authentication token
        :param kwargs: ...
        :return: request object
        """
        if headers is None:
            headers = {}
        if self.token:  # if the authentication token is given
            headers['Authorization'] = "Token {}".format(self.token)
        return requests.request(method, url, headers=headers, **kwargs)

    def head(self, url, **kwargs):  # head request
        return self.request('head', url, **kwargs)

    def post(self, url, **kwargs):  # post request
        return self.request('post', url, **kwargs)

    def get(self, url, **kwargs):  # get request
        return self.request('get', url, **kwargs)

    def put(self, url, **kwargs):  # put request
        return self.request('put', url, **kwargs)

    def doc_status(self, tool, doc_id):
        """
        Gets the status of a document from the server. HEAD request

        :param tool: specific NLP tool
        :param doc_id: id of the document
        :return: status of the document (e.g., PENDING, STARTED, DONE, ERROR)
        """
        url = "{self.server}/api/tools/{tool}/{doc_id}".format(**locals())  # endpoint
        res = self.head(url)  # get the status
        if res.status_code == 403:
            raise Exception("403 Forbidden, please provide a token")
        if 'Status' in res.headers:
            return res.headers['Status']
        raise Exception("Cannot determine status for {tool}/{doc_id}; return code: {res.status_code}"
                        .format(**locals()))

    def process(self, tool, doc, doc_id=None, **kwargs):
        """
        Sends the document for processing by the NLP tool

        :param tool: name of the specific NLP tool
        :param doc: the text document
        :param doc_id: id of the document (optional)
        :param kwargs: -
        :return: results of the POST request (doc_id)
        """
        url = "{self.server}/api/tools/{tool}/".format(**locals())  # endpoint
        if doc_id is not None:
            url = "{url}?doc_id={doc_id}".format(**locals())
        res = self.post(url, data=doc.encode("utf-8"))  # POST document for processing
        if res.status_code != 202:
            raise Exception("Error on processing doc with {tool}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.headers['ID']

    def result(self, tool, doc_id, return_format=None):
        """
        Gets the result of the processing on the document, if specified in the return_format (e.g., json)
        :param tool: name of the specific NLP tool
        :param doc_id: id of the document
        :param return_format: preferred return format (e.g., json)
        :return: result of the processed document in the indicated format
        """
        url = "{self.server}/api/tools/{tool}/{doc_id}".format(**locals())  # endpoint
        if return_format is not None:
            url = "{url}?return_format={return_format}".format(**locals())
        res = self.get(url)  # get the result
        if res.status_code != 200:
            raise Exception("Error on getting result for {tool}/{doc_id}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.text

    def get_task(self, tool):
        """
        Get the task (in case this is called by the worker)
        :param tool: name of the specific NLP tool
        :return: document id, and text
        """
        url = "{self.server}/api/tools/{tool}/".format(**locals())  # endpoint
        res = self.get(url)  # GET request

        if res.status_code == 404:
            return None, None
        elif res.status_code != 200:
            raise Exception("Error on getting a task for {tool}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.headers['ID'], res.text

    def store_result(self, tool, doc_id, result):
        """
        Sends the result of the NLP processing on the document to the server
        :param tool: name of the specific NLP tool
        :param doc_id: id of the document
        :param result: processed text
        :return: -
        """
        url = "{self.server}/api/tools/{tool}/{doc_id}".format(**locals())  # endpoint
        data = result.encode("utf-8")  # encoding the doc
        res = self.put(url, data=data)  # PUT request

        if res.status_code != 204:
            raise Exception("Error on storing result for {tool}:{doc_id}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))

    def store_error(self, tool, doc_id, result):
        """
        Sends the error of the NLP processing on the document to the server
        :param tool: name of the specific NLP tool
        :param doc_id: id of the document
        :param result: processed text (with error)
        :return: -
        """
        url = "{self.server}/api/tools/{tool}/{doc_id}".format(**locals())  # endpoint
        data = result.encode("utf-8")  # endocing
        headers = {'Content-type': ERROR_MIME}  # ERROR MIME
        res = self.put(url, data=data, headers=headers)  # PUT request
        if res.status_code != 204:
            raise Exception("Error on storing error for {tool}:{doc_id}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))

    def bulk_doc_status(self, tool, doc_ids):
        """
        Sends the status of multiple documents

        :param tool: name of the specific NLP tool
        :param doc_ids: document ids
        :return: json containing the statuses
        """
        url = "{self.server}/api/tools/{tool}/bulk/status".format(**locals())  # endpoint
        res = self.post(url, json=doc_ids)  # POST request
        if res.status_code != 200:
            raise Exception("Error on getting bulk status for {tool}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.json()

    def bulk_doc_result(self, tool, doc_ids, return_format=None):
        url = "{self.server}/api/tools/{tool}/bulk/result".format(**locals())
        if return_format is not None:
            url = "{url}?format={format}".format(**locals())
        res = self.post(url, json=doc_ids)
        if res.status_code != 200:
            raise Exception("Error on getting bulk results for {tool}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.json()

    def bulk_process(self, tool, docs, doc_ids=None, reset_error=False, reset_pending=False):
        url = ("{self.server}/api/tools/{tool}/bulk/process?reset_error={reset_error}&reset_pending={reset_pending}"\
               .format(**locals()))
        body = list(docs) if doc_ids is None else dict(zip(doc_ids, docs))
        res = self.post(url, json=body)
        if res.status_code != 200:
            raise Exception("Error on bulk process for {tool}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.json()