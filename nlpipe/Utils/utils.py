import hashlib

# Status definitions and subdir names
STATUS = {"PENDING": "queue",
          "STARTED": "inprogress",
          "DONE": "results",
          "ERROR": "errors"}


def get_id(tool, doc):
    """
    Calculate the id (hash) of the given document
    :param tool: name of the tool (string)
    :param doc: The document (string)
    :return: a task id (hash)
    """
    if len(doc) == 34 and doc.startswith("0x"):  # it sure looks like a hash
        return doc
    m = hashlib.md5()  # md5 hash generator
    if isinstance(doc, str):
        doc = doc.encode("utf-8")  # encoding
    m.update(doc)  # generating the hash
    if isinstance(tool, str):
        tool = tool.encode("utf-8")  # encoding
    m.update(tool)  # generating the hash
    return "0x" + m.hexdigest()
