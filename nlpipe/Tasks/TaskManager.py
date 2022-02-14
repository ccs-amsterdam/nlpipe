import logging
from nlpipe.Tasks.DatabaseTaskManager import Task, Docs
from peewee import *


class TaskManager:
    def __init__(self, app_server):
        self.app = app_server  # the RESTserver

    def process(self, tool, doc, task_idx=None, document_idx=None, reset_error=False, reset_pending=False):
        if task_idx is None:
            logging.debug("New task just arrived, storing the task and document")
            task_id = Task.insert({'tool': tool, 'status': "PENDING"}).execute()  # adding the task to db
            doc_id, path = self.app.docStorageModule.process(tool=tool, doc=doc, task_id=task_id,
                                                             doc_id=document_idx, doc_status="PENDING")  # stores the doc
            Docs.insert({'doc_id': doc_id,
                         'task_id': task_id, 'path': path, 'status': "PENDING"}).execute()  # adding the doc to the db
            logging.debug("New task recorded with task_id: {task_id} and doc_id: {doc_id}".format(**locals()))
            return task_id, doc_id

        doc_status = self.get_doc_status(document_idx)
        if (doc_status == "ERROR" and reset_error) or (doc_status == "STARTED" and reset_pending):
            logging.debug("Re-assigning doc {doc_id} with status {status} to {tool}".format(**locals()))
            Docs.update({Docs.status: "PENDING"}).where(Docs.doc_id == document_idx).execute()  # update the status of the doc in db

        return task_idx, document_idx

    @staticmethod
    def get_doc_status(doc_id):
        try:
            return Docs.get(Docs.doc_id == doc_id).status  # return the status of a document
        except Docs.DoesNotExist:
            return "UNKNOWN"

    def get_task(self, tool):
        try:
            doc_id = Docs.get(Docs.status == "PENDING").doc_id  # gets the first document where status is "PENDING"
            doc_id, doc = self.app.docStorageModule.get_task(tool, doc_id)
            Docs.update(status="STARTED").where(Docs.doc_id == doc_id).execute()  # updates the status of the doc
            return doc_id, doc
        except DoesNotExist:  # if Docs.DoesNotExist: there are no documents with "PENDING" status
            return None, None

    def store_result(self, tool, doc_id, doc):
        self.app.docStorageModule.store_result(tool, doc_id, doc)
        Docs.update(status="DONE").where(Docs.doc_id == doc_id).execute()  # update the status in the database

    def store_error(self, tool, doc_id, doc):
        self.app.docStorageModule.store_error(tool, doc_id, doc)
        Docs.update(doc_id="ERROR").where(Docs.doc_id == doc_id).execute()  # update the status in the database

    @staticmethod
    def get_task_status(task_id):
        status = Task.get(Task.id == task_id).status
        return status

    def get_result(self, tool, doc_id, ret_format):
        res = self.app.docStorageModule.result(tool, doc_id, return_format=ret_format)
        return res
