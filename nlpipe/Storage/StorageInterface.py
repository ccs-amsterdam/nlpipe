import itertools


class StorageInterface(object):
    """Abstract class for NLPipe client bindings"""

    def process(self, tool, doc, doc_id=None, task_id=None, doc_status="UNKNOWN", reset_error=False, reset_pending=False):
        """Add a document to be processed by module, returning the task ID
        :param tool: tool name
        :param doc: A document (string)
        :param task_id: An optional id for the task
        :param doc_id: An optional id for the document
        :param doc_status: An optional status for the document
        :param reset_error: Re-assign documents that have status 'ERROR'
        :param reset_pending: Re-assign documents that have status 'PENDING'
        :return: task ID
        :rtype: str
        """
        raise NotImplementedError()

    def result(self, tool, doc_id, return_format=None):
        """Get processing result, optionally converted to a specified format.
        If the status is ERROR, the result will be raised as an exception
        :param tool: tool name
        :param doc_id: A document (string)
        :param return_format: (Optional) format to convert to, e.g. 'xml', 'csv', 'json'
        :return: The result of processing (string)
        """
        raise NotImplementedError()

    # def process_inline(self, module, doc, format=None, id=None):
    #     """
    #     Process the given document, use cached version if possible, wait and return result
    #     :param module: Module name
    #     :param doc: A document (string)
    #     :return: The result of processing (string)
    #     """
    #     if id is None:
    #         id = get_id(doc)
    #     if self.status(module, id) == 'UNKNOWN':
    #         self.process(module, doc, id)
    #     while True:
    #         status = self.status(module, id)
    #         if status in ('DONE', 'ERROR'):
    #             return self.result(module, id, format=format)
    #         time.sleep(0.1)

    def get_task(self, tool, doc_id):
        """
        Get a document to process with the given module, marking the document as 'in progress'
        :param tool: tool of the module
        :param doc_id: id of the document
        :return: a pair (id, string) for the document to be processed
        """
        raise NotImplementedError()

    # def get_tasks(self, module, n):
    #     """
    #     Get multiple documents to process
    #     :param module: Name of the module for processing
    #     :param n: Number of documents to retrieve
    #     :return: a sequence of (id, document string) pairs
    #     """
    #     for i in range(n):
    #         yield self.get_task(module)

    def store_result(self, tool, doc_id, result):
        """
        Store the given result
        :param tool: tool name
        :param doc_id: Document
        :param result: Result (string)
        """
        raise NotImplementedError()

    def store_error(self, tool, doc_id, result):
        """
        Store an error
        :param tool: tool name
        :param doc_id: Document
        :param result: Result (string) describing the error
        """
        raise NotImplementedError()

    # def bulk_status(self, module, ids):
    #     """Get processing status of multiple ids
    #     :param module: Module name
    #     :param ids: Task IDs
    #     :return: a dict of {id: status}
    #     """
    #     return {id: self.status(module, id) for id in ids}
    #
    # def bulk_result(self, module, ids, format=None):
    #     """Get results for multiple ids
    #     :param module: Module name
    #     :param ids: Task IDs
    #     :return: a dict of {id: result}
    #     """
    #     return {id: self.result(module, id, format=format) for id in ids}
    #
    # def bulk_process(self, module, docs, ids=None, **kargs):
    #     """
    #     Add multiple documents to the processing queue
    #     :param module:  Module name
    #     :param docs: Documents to process
    #     :param ids: Optional sequence of explicit IDs corresponding to the documents
    #     :param kargs: Additional options to pass to process
    #     :return: a sequence of IDs
    #     """
    #     if ids is None:
    #         ids = itertools.repeat(None)
    #     return [self.process(module, doc, id=id, **kargs) for (doc, id) in zip(docs, ids)]
