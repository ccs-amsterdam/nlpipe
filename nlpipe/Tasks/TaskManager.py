from nlpipe.Tasks.DatabaseTaskManager import Task, Docs


class TaskManager():
    def __init__(self):
        pass

    def process(self, tool, doc, task_id=None, doc_id=None):
        if task_id == None:
            task_id = Task.insert({'tool': tool, 'status': "PENDING"}).execute()  # adding the task to db

            Docs.insert({'doc_id': doc_id, 'task_id': task_id,
                         'path': self._filename(tool, doc_id),
                         'status': "PENDING"}).execute()  # adding the doc to the db

            doc_id = app_restServer.docStorageModule.process(tool,
                                                         doc, doc_id,  # in case it is given
                                                         task_id=task_id)  # stores the doc, and if needed generates the id

        return task_id, doc_id
