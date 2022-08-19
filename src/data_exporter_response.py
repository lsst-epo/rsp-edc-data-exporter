# For creating robust responses from the RSP Data Exporter service

class DataExporterResponse():
    status = ""
    messages = []
    manifest_url = ""

    def __init__(self, status = None, messages = [], manifest_url = None):
        self.status = status
        self.messages = messages
        self.manifest_url = manifest_url