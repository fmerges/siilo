# -*- coding: utf-8 -*-
import locale
import os
try:
    from unittest import mock
except ImportError:
    import mock

from cmislib import Repository
from cmislib.exceptions import ObjectNotFoundException, RuntimeException
import pytest

from siilo.exceptions import FileNotFoundError


@pytest.fixture
def repository():
    return mock.MagicMock(name='repository', spec=Repository)


@pytest.fixture
def storage(repository):
    from siilo.storages.cmis import CmisStorage
    return CmisStorage(repository=repository)


@pytest.fixture
def object_does_not_exist():
    return ObjectNotFoundException(
        status=mock.sentinel.status,
        url=mock.sentinel.url,
    )


@pytest.fixture
def object_does_not_exist_on_delete():
    return RuntimeException(
        status=mock.sentinel.status,
        url=mock.sentinel.url,
    )


def test_storage_repr(storage, repository):
    expected = '<CmisStorage repository={0!r}>'.format(repository)
    assert repr(storage) == expected


def test_delete_removes_the_file(storage, repository):
    storage.delete('some_file.txt')
    repository.getObjectByPath.assert_called_with('some_file.txt')
    obj = repository.getObjectByPath('some_file.txt')
    assert obj.delete.called


def test_delete_raises_error_if_file_doesnt_exist_on_get_object(
    storage, repository, object_does_not_exist
):
    repository.getObjectByPath.side_effect = object_does_not_exist
    with pytest.raises(FileNotFoundError) as excinfo:
        storage.delete('some_file.txt')
    assert excinfo.value.name == 'some_file.txt'


def test_delete_raises_error_if_file_doesnt_exist_on_delete(
    storage, repository, object_does_not_exist_on_delete
):
    obj = repository.getObjectByPath('some_file.txt')
    obj.delete.side_effect = object_does_not_exist_on_delete
    with pytest.raises(FileNotFoundError) as excinfo:
            storage.delete('some_file.txt')
    assert excinfo.value.name == 'some_file.txt'


def test_exists_returns_true_if_file_exists(storage, repository):
    assert storage.exists('some_file.txt') is True
    repository.getObjectByPath.assert_called_with('some_file.txt')


def test_size_returns_file_size_in_bytes(storage, repository):
    expected_size = 4
    obj = repository.getObjectByPath('some_file.txt')
    obj.properties.get.return_value = expected_size
    assert storage.size('some_file.txt') == expected_size


def test_size_raises_error_if_file_doesnt_exist(
    storage, repository, object_does_not_exist
):
    repository.getObjectByPath.side_effect = object_does_not_exist
    with pytest.raises(FileNotFoundError) as excinfo:
        storage.size('some_file.txt')
    assert excinfo.value.name == 'some_file.txt'


def test_url_raises_not_implemented(storage, repository):
    with pytest.raises(NotImplementedError) as excinfo:
        storage.url('some_file.txt')
    assert excinfo.type == NotImplementedError


def test_open_returns_cmis_file_with_default_mode_and_encoding(storage):
    with mock.patch(
        'siilo.storages.cmis.CmisFile'
    ) as MockFile:
        MockFile.return_value = mock.sentinel.file
        file_ = storage.open('some_file.txt')
        MockFile.assert_called_with(
            storage=storage,
            name='some_file.txt',
            mode='r',
            encoding=None
        )
        assert file_ is mock.sentinel.file


def test_open_returns_cmis_file_with_given_mode_and_encoding(storage):
    with mock.patch(
        'siilo.storages.cmis.CmisFile'
    ) as MockFile:
        MockFile.return_value = mock.sentinel.file
        file_ = storage.open('some_file.txt', 'w', 'utf-8')
        MockFile.assert_called_with(
            storage=storage,
            name='some_file.txt',
            mode='w',
            encoding='utf-8'
        )
        assert file_ is mock.sentinel.file


@pytest.mark.parametrize(
    'mode', ['a', 'a+', 'a+b', 'ab', 'r', 'r+', 'r+b', 'rb']
)
def test_downloads_file_for_read_and_append_modes(storage, repository, mode):
    contents = b'Quick brown fox jumps over lazy dog'

    obj = repository.getObjectByPath('/some_file.txt')
    obj.getContentStream.return_value = iter([contents])

    with storage.open('some_file.txt', mode) as file_:
        with open(file_._temporary_filename, 'rb') as tempfile:
            assert tempfile.read() == contents


@pytest.mark.parametrize('mode', ['w', 'wb', 'w+', 'w+b'])
def test_doesnt_download_file_for_write_modes(storage, repository, mode):
    with storage.open('some_file.txt', mode):
        assert not repository.getObjectByPath.called
    # when closing it gets downloaded because it has to check if the file
    # exists in order to change the contents, creating a new version
    assert repository.getObjectByPath.called


@pytest.mark.parametrize(
    ('mode', 'encoding'),
    [
        ('r', None),
        ('w', 'UTF-8'),
    ]
)
def test_opens_temporary_file_with_given_mode_and_encoding(
    storage, mode, encoding
):
    with storage.open('some_file.txt', mode, encoding) as file_:
        assert file_._stream.name == file_._temporary_filename
        assert file_._stream.mode == mode
        assert file_._stream.encoding == encoding or locale.getdefaultlocale()


@pytest.mark.parametrize(
    'filename',
    [
        '/etc/passwd',
        '../etc/passwd',
    ]
)
def test_always_opens_temporary_file_within_the_temporary_directory(
    storage, filename
):
    with storage.open(filename) as file_:
        assert file_._temporary_filename == os.path.abspath(
            file_._temporary_filename
        )
        assert file_._temporary_filename.startswith(file_._temporary_directory)


def test_removes_temporary_directory_after_file_is_closed(storage, repository):
    with storage.open('some_file.txt', 'r') as file_:
        pass
    assert not os.path.exists(file_._temporary_directory)


@pytest.mark.parametrize(
    ('method_name', 'method_args', 'method_returns'),
    [
        ('fileno', [], True),
        ('flush', [], False),
        ('isatty', [], True),
        ('read', [], True),
        ('readable', [], True),
        ('readall', [], True),
        ('readinto', [], True),
        ('readline', [], True),
        ('readlines', [], True),
        ('seekable', [], True),
        ('tell', [], False),
        ('writable', [], True),
        ('write', ['foo'], False),
        ('writelines', [['foo', 'bar']], False),
    ]
)
def test_delegates_file_api_methods_to_underlying_temporary_file(
    storage, method_name, method_args, method_returns
):
    with storage.open('some_file.txt', 'r') as file_:
        file_._stream = mock.MagicMock(name='stream')

        method = getattr(file_, method_name)
        rv = method(*method_args)

    stream_method = getattr(file_._stream, method_name)
    stream_method.assert_called_with(*method_args)
    if method_returns:
        assert rv == stream_method(*method_args)


@pytest.mark.parametrize(
    'property_name',
    [
        'closed',
        'encoding',
        'mode',
    ]
)
def test_delegates_file_api_properties_to_underlying_temporary_file(
    storage, property_name
):
    with storage.open('some_file.txt', 'r') as file_:
        file_._stream = mock.MagicMock(name='stream')

        actual_value = getattr(file_, property_name)
        expected_value = getattr(file_._stream, property_name)

    assert actual_value == expected_value


def test_reclosing_file_is_noop(storage):
    with storage.open('some_file.txt', 'r') as file_:
        pass
    file_.close()


def test_can_iterate_over_file(storage, repository):
    contents = b'Quick brown fox\njumps over lazy dog'

    obj = repository.getObjectByPath('some_file.txt')
    obj.getContentStream.return_value = iter([contents])

    with storage.open('some_file.txt', 'r') as file_:
        lines = list(line for line in file_)

    assert lines == [
        u'Quick brown fox\n',
        u'jumps over lazy dog',
    ]


@pytest.mark.parametrize(
    ('mode', 'encoding', 'expected_format'),
    [
        (
            'r',
            'UTF-8',
            (
                '<CmisFile storage={storage!r}, name={name!r}, '
                'mode={mode!r}, encoding={encoding!r}>'
            )
        ),
        (
            'rb',
            None,
            '<CmisFile storage={storage!r}, name={name!r}, mode={mode!r}>'
        ),
    ]
)
def test_cmisfile_repr(storage, mode, encoding, expected_format):
    with storage.open('some_file.txt', mode, encoding) as file_:
        assert repr(file_) == expected_format.format(
            storage=storage,
            name=file_.name,
            mode=file_.mode,
            encoding=encoding
        )


@pytest.mark.parametrize(
    'mode',
    ['a', 'a+', 'a+b', 'ab', 'w', 'wb', 'w+', 'w+b']
)
def test_uploads_file_opened_in_write_mode_but_new_for_storage(
    storage, repository, mode, object_does_not_exist
):
    repository.getObjectByPath.side_effect = object_does_not_exist
    with mock.patch('siilo.storages.cmis.io.open') as mock_open:
        mock_open.return_value = mock.MagicMock(closed=False)
        with storage.open('some_file.txt', mode) as file_:
            pass
    with mock_open(file_._temporary_filename, mode='rb') as temp_file:
        repository.rootFolder.createDocument.assert_called_with(
            'some_file.txt', contentFile=temp_file)


@pytest.mark.parametrize(
    'mode',
    ['a', 'a+', 'a+b', 'ab', 'w', 'wb', 'w+', 'w+b']
)
def test_uploads_file_with_subfolders_opened_in_write_mode_but_new_for_storage(
    storage, repository, mode, object_does_not_exist
):
    repository.getObjectByPath.side_effect = object_does_not_exist
    with mock.patch('siilo.storages.cmis.io.open') as mock_open:
        mock_open.return_value = mock.MagicMock(closed=False)
        with storage.open('/folder/some_file.txt', mode) as file_:
            pass
    with mock_open(file_._temporary_filename, mode='rb') as temp_file:
        repository.rootFolder.createFolder.assert_called_with('folder')
        repository.rootFolder.createFolder.return_value.createDocument \
            .assert_called_with('some_file.txt', contentFile=temp_file)


@pytest.mark.parametrize(
    'mode',
    ['a', 'a+', 'a+b', 'ab', 'r+', 'r+b', 'w', 'wb', 'w+', 'w+b']
)
@pytest.mark.parametrize(
    ('method_name', 'method_args'),
    [
        ('write', 'foo'),
        ('writelines', ['foo', 'bar']),
    ]
)
def test_uploads_file_already_on_storage(
    storage, repository, mode, method_name, method_args
):
    with mock.patch('siilo.storages.cmis.io.open') as mock_open:
        mock_open.return_value = mock.MagicMock(closed=False)
        with storage.open('some_file.txt', mode) as file_:
            method = getattr(file_, method_name)
            method(method_args)
    with mock_open(file_._temporary_filename, mode='rb') as temp_file:
        file_.cmis_obj.setContentStream.assert_called_with(temp_file)
