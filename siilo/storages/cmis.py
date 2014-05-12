# -*- coding: utf-8 -*-
"""
    siilo.storages.filesystem
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    :copyright: (c) 2014 by Janne Vanhala.
    :license: MIT, see LICENSE for more details.
"""
import io
import os
import shutil
import tempfile

from cmislib.exceptions import ObjectNotFoundException, RuntimeException

from ..exceptions import FileNotFoundError
from .base import Storage


class CmisStorage(Storage):
    """
    A storage for a `Content Management Interoperability Services`_ (CMIS)
    compatible CMS.

    .. _Content Management Interoperability Services:
            http://chemistry.apache.org/project/cmis.html

    In order to use this storage driver you need to have Apache Chemistry
    CmisLib installed. You can install it using pip::

        pip install cmislib

    Example::

        import cmislib
        from siilo.storages.cmis import CmisStorage

        client = CmisClient(
            'http://cmis.alfresco.com/s/cmis', 'admin', 'admin')
        repository = client.defaultRepository

        storage = CmisStorage(repository)

        with storage.open('hello.txt', 'w') as f:
            f.write('Hello World!')

    :param repository:
        the :class:`cmislib.Repository` used by this storage for file
        operations.
    """
    def __init__(self, repository):
        self.repository = repository

    def _get_object(self, name):
        try:
            return self.repository.getObjectByPath(name)
        except ObjectNotFoundException:
            raise FileNotFoundError(name)

    def delete(self, name, all_versions=False):
        obj = self._get_object(name)
        try:
            obj.delete(allVersions=all_versions)
            # If we try to delete a document which is already removed on the
            # server side it will return a RunTimeException with HTTP Error
            # Code 500
        except RuntimeException:
            raise FileNotFoundError(name)

    def exists(self, name):
        try:
            self._get_object(name)
        except FileNotFoundError:
            return False
        return True

    def open(self, name, mode='r', encoding=None):
        return CmisFile(
            storage=self,
            name=name,
            mode=mode,
            encoding=encoding
        )

    def size(self, name):
        obj = self._get_object(name)
        return obj.properties.get('cmis:contentStreamLength')

    def __repr__(self):
        return '<CmisStorage repository={0!r}>'.format(self.repository)


class CmisFile(object):
    """
    A file like object for abstracting operations with the
    :class:`CmisStorage` class.

    Example::

        with storage.open('/folder/subfolder/file.txt') as f:
            content = f.readlines()
        title = f.cmis_object.title
        properties = f.cmis_object.properties

    :param storage:
        the :class:`CmisStorage` instance.

    :param name:
        name of the remote file, can include directories. Adds a leading slash
        if missing.

    :param mode:
        file mode.

    :param encoding:
        file encoding.
    """
    def __init__(self, storage, name, mode='r', encoding=None):
        self.storage = storage
        #: :class:`cmislib.Document` object. Can be used for accessing
        #: properties of the document. See example above.
        self.cmis_object = None
        if not name.startswith('/'):
            name = '/' + name
        self._name = name

        self._should_download = 'r' in mode or 'a' in mode
        self._has_changed = 'w' in mode

        self._open(mode, encoding)

    def _open(self, mode, encoding):
        self._make_temporary_directory()

        if self._should_download:
            self._download_or_mark_changed(mode)

        self._stream = io.open(
            self._temporary_filename,
            mode=mode,
            encoding=encoding
        )

    def close(self):
        if not self.closed:
            self._stream.close()
            if self._has_changed:
                self._upload()
            self._remove_temporary_directory()

    @property
    def name(self):
        return self._name

    def read(self):
        return self._stream.read()

    def write(self, data):
        self._has_changed = True
        self._stream.write(data)

    def writelines(self, lines):
        self._has_changed = True
        self._stream.writelines(lines)

    closed = property(lambda self: self._stream.closed)
    encoding = property(lambda self: self._stream.encoding)
    fileno = property(lambda self: self._stream.fileno)
    flush = property(lambda self: self._stream.flush)
    isatty = property(lambda self: self._stream.isatty)
    mode = property(lambda self: self._stream.mode)
    readable = property(lambda self: self._stream.readable)
    readall = property(lambda self: self._stream.readall)
    readinto = property(lambda self: self._stream.readinto)
    readline = property(lambda self: self._stream.readline)
    readlines = property(lambda self: self._stream.readlines)
    seekable = property(lambda self: self._stream.seekable)
    tell = property(lambda self: self._stream.tell)
    writable = property(lambda self: self._stream.writable)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self):
        return iter(self._stream)

    def __repr__(self):
        args = [
            ('storage', self.storage),
            ('name', self.name),
            ('mode', self.mode),
        ]
        if hasattr(self, 'encoding'):
            args.append(('encoding', self.encoding))
        args = ', '.join(
            '{key}={value!r}'.format(key=key, value=value)
            for key, value in args
        )
        return '<CmisFile {args}>'.format(args=args)

    def _make_temporary_directory(self):
        self._temporary_directory = tempfile.mkdtemp()

    def _remove_temporary_directory(self):
        shutil.rmtree(self._temporary_directory)

    @property
    def _temporary_filename(self):
        return os.path.join(
            self._temporary_directory,
            os.path.basename(self.name)
        )

    def _download_or_mark_changed(self, mode):
        try:
            self._download()
        except FileNotFoundError:
            if 'a' in mode:
                self._has_changed = True
            else:
                raise

    def _download(self):
        with io.open(self._temporary_filename, mode='wb') as f:
            self.cmis_obj = self.storage._get_object(self.name)
            for data in self.cmis_obj.getContentStream():
                f.write(data)

    def _upload(self):
        with io.open(self._temporary_filename, mode='rb') as f:
            try:
                self.cmis_obj = self.storage._get_object(self.name)
                self.cmis_obj.setContentStream(f)
            except FileNotFoundError:
                obj = self.storage.repository.rootFolder
                dirpath, filename = os.path.split(self.name)
                for dirname in dirpath.split('/'):
                    if dirname:
                        folder_obj = None
                        rs = obj.getTree()
                        for d in rs.getResults():
                            if d.getName() == dirname:
                                folder_obj = d
                                break
                        if not folder_obj:
                            obj = obj.createFolder(dirname)
                        else:
                            obj = folder_obj
                self.cmis_obj = obj.createDocument(filename, contentFile=f)
