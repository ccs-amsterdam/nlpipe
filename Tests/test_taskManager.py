import tempfile
from nlpipe.Tasks.TaskManager import TaskManager
from nlpipe.ServerTypes.RESTServer import app_restServer
from nlpipe.Storage.FileSystemStorage import FileSystemStorage
from nlpipe.Tools.test_upper import TestUpper
from nlpipe.Tasks.DatabaseTaskManager import initialize_if_needed


def test_task_manager(num_docs=5):
    task_manager = TaskManager(app_server=app_restServer)
    app_restServer.docStorageModule = FileSystemStorage(task_manager=task_manager,
                                                        result_dir=tempfile.TemporaryDirectory(prefix="nlpipe_").name)  # add FileSystemStorage to the REST Server

    initialize_if_needed()

    to_process = [task_manager.process(TestUpper.name, "This is a test {i}".format(**locals())) for i in range(num_docs)]

    task_id, doc_id = to_process[0]
    assert task_manager.get_task_status(task_id) == "PENDING"
    assert task_manager.get_doc_status(doc_id) == "PENDING"
    task_doc_id, doc = task_manager.get_task('test_upper')
    assert task_doc_id == doc_id
    assert doc == "This is a test 0"
    assert task_manager.get_doc_status(doc_id) == "STARTED"

    task_id, doc_id = to_process[1]
    task_doc_id, doc = task_manager.get_task('test_upper')
    assert task_doc_id == doc_id
    assert doc == "This is a test 1"
    assert task_manager.get_doc_status(doc_id) == "STARTED"

    TU = TestUpper()
    assert TU.process(text=doc, additional_arguments=None) == doc.upper()

    task_manager.store_result(TestUpper.name, doc_id, TU.process(text=doc, additional_arguments=None))
    assert task_manager.get_result(tool=TestUpper.name, doc_id=doc_id, ret_format=None) == doc.upper()
