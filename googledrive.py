"""Simple Google Drive API wrapper."""

__version__ = '0.0.1'
__author__ = 'skitschy'

from time import sleep
from functools import reduce

from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from googleapiclient.errors import HttpError


class GoogleDrive:
    """Simple Google Drive API wrapper class.

    >>> from googledrive import GoogleDrive
    >>> with GoogleDrive(credentials) as gdrive:
    >>>   file_content = gdrive.read(['dirA', 'subdir1'], 'filename')
    >>>   gdrive.write(['dirA', 'subdir2'], 'filename.bak', file_content, 'text/plain')
    """

    SCOPE = 'https://www.googleapis.com/auth/drive'

    def __init__(self, credentials, max_retry=3, retry_interval=1):
        self.max_retry = max_retry
        self.retry_interval = retry_interval
        self.service = self.__retry(
            lambda: build('drive', 'v3', credentials=credentials)
        )
        self.drivefiles = self.__retry(
            lambda: self.service.files()
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.service.close()

    def read(self, path, name):
        if isinstance(path, str):
            parent_id = path
        else:
            parent_id = self.get_path_id(path)
        fileid = self.get_id(parent_id, name)
        return self.read_file_id(fileid) if fileid else None

    def write(self, path, name, content, mimetype):
        if isinstance(path, str):
            parent_id = path
        else:
            parent_id = self.get_path_id(path)
        fileid = self.get_id(parent_id, name)
        if fileid:
            self.update_file_id(fileid, content, mimetype)
            return fileid
        else:
            return self.create_file(parent_id, name, content, mimetype)

    def list(self, path=None, query=None, fields=None):
        if isinstance(path, str):
            parent_id = path
        elif path:
            parent_id = self.get_path_id(path)
        else:
            parent_id = None
        return list(self.each_files(parent_id, query, fields))

    def each_files(self, parent_id=None, query=None, fields=None):
        if parent_id:
            if query:
                q = f"'{parent_id}' in parents and {query}"
            else:
                q = f"'{parent_id}' in parents"
        else:
            if query:
                q = query
            else:
                q = ''
        if fields and 'nextPageToken' in fields:
            fields = 'nextPageToken,' + fields
        page_token = None
        while True:
            request = self.drivefiles.list(
                q=q, spaces='drive', fields=fields, pageToken=page_token
            )
            response = self.__execute(request)
            for file in response.get('files', []):
                yield file
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    def get_path_id(self, path, root_id='root'):
        return reduce(lambda parent, name: self.get_id(parent, name),
                      path, root_id)

    def get_id(self, parent_id, name):
        request = self.drivefiles.list(
            q=f"'{parent_id}' in parents and name='{name}'",
            spaces='drive', fields="files(id)")
        files = self.__execute(request).get('files', [])
        return next(iter(files), {}).get('id', None)

    def create_file(self, parent_id, name, content, mimetype):
        metadata = {'name': name, 'parents': [parent_id]}
        media = MediaInMemoryUpload(content, mimetype=mimetype)
        request = self.drivefiles.create(body=metadata, media_body=media)
        return self.__execute(request).get('id', None)

    def read_file_id(self, file_id):
        return self.__execute(self.drivefiles.get_media(fileId=file_id))

    def update_file_id(self, file_id, content, mimetype):
        media = MediaInMemoryUpload(content, mimetype=mimetype)
        request = self.drivefiles.update(fileId=file_id, media_body=media)
        self.__execute(request)

    def delete_file_id(self, file_id):
        self.__execute(self.drivefiles.delete(fileId=file_id))

    def __execute(self, request):
        return self.__retry(lambda: request.execute())

    def __retry(self, function):
        ctry = 0
        while True:
            try:
                return function()
            except (TimeoutError, HttpError):
                if ctry == self.max_retry:
                    raise
                else:
                    ctry += 1
                    sleep(self.retry_interval)
