from pathlib import Path
from types import SimpleNamespace as N
from unittest.mock import Mock
import json

import pytest
import requests

from nba_orderbook.kaggle import Backend, FINAL_FILES, UPLOAD_FILES, OutcomeUnknown, SubmissionNotAttempted, notebook_source


def http_error(code):
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError('test', response=response)


def client(datasets=None, kernels=None):
    return N(datasets=N(dataset_api_client=datasets or Mock()),
             kernels=N(kernels_api_client=kernels or Mock()))


def ready(ref='owner/game', version=1, files=FINAL_FILES):
    return {'ref': ref, 'version': version, 'status': 'READY', 'files': list(files)}


def final_files(directory):
    for name in UPLOAD_FILES:
        (directory / name).write_text('{}')


def test_timeout_submission_queries_same_reference_and_does_not_repeat(tmp_path):
    api = Mock()
    api.save_kernel.side_effect = requests.Timeout()
    backend = Backend('owner', client=client(kernels=api))
    api.list_kernels.return_value = N(kernels=[], next_page_token='')
    backend.snapshot = Mock(return_value={'ref': 'owner/nba-batch-batch', 'version': 1,
                                         'status': 'RUNNING', 'batch_id': 'batch'})
    (tmp_path/'src/nba_orderbook').mkdir(parents=True)
    (tmp_path/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    result = backend.submit({'id': 'batch'}, tmp_path)
    assert result['version'] == 1
    assert api.save_kernel.call_count == 1
    assert backend.snapshot.call_args.args == ('owner/nba-batch-batch',)


def test_existing_matching_instance_is_not_submitted_again(tmp_path):
    api = Mock()
    backend = Backend('owner', client=client(kernels=api))
    api.list_kernels.return_value = N(kernels=[N(ref='owner/nba-batch-batch')], next_page_token='')
    backend.snapshot = Mock(return_value={'batch_id': 'batch', 'status': 'RUNNING'})
    assert backend.submit({'id': 'batch'}, tmp_path)['status'] == 'RUNNING'
    api.save_kernel.assert_not_called()


def test_missing_after_timeout_is_unknown_not_a_second_submit(tmp_path):
    api = Mock()
    api.save_kernel.side_effect = requests.Timeout()
    backend = Backend('owner', client=client(kernels=api))
    api.list_kernels.return_value = N(kernels=[], next_page_token='')
    backend.snapshot = Mock(side_effect=http_error(403))
    (tmp_path/'src/nba_orderbook').mkdir(parents=True)
    (tmp_path/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    with pytest.raises(OutcomeUnknown):
        backend.submit({'id': 'batch'}, tmp_path)
    assert api.save_kernel.call_count == 1


def test_qa_notebook_has_no_runtime_credentials(tmp_path, monkeypatch):
    package = tmp_path/'src/nba_orderbook'
    package.mkdir(parents=True)
    (package/'__init__.py').write_text('')
    (tmp_path/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    monkeypatch.setenv('KAGGLE_API_TOKEN', 'must-not-be-included')
    source = notebook_source({'id': 'batch'}, tmp_path)
    assert 'must-not-be-included' not in source
    assert '# nba_batch_id: batch' in source
    code = json.loads(source)['cells'][0]['source']
    compile(code, '<notebook>', 'exec')
    assert "'3.12'" in code


def test_publish_timeout_reconciles_ready_without_duplicate_creation(tmp_path):
    final_files(tmp_path)
    api = Mock()
    api.create_dataset.side_effect = requests.Timeout()
    backend = Backend('owner', client=client(datasets=api), ready_timeout=0)
    backend.inspect_dataset = Mock(side_effect=[None, ready()])
    backend._upload = Mock(return_value=__import__('kagglesdk.datasets.types.dataset_api_service', fromlist=['ApiDatasetNewFile']).ApiDatasetNewFile())
    result = backend.publish(tmp_path, 'owner/game')
    assert result['status'] == 'READY'
    assert api.create_dataset.call_count == 1
    api.create_dataset_version.assert_not_called()
    assert json.loads((tmp_path/'.publish-state.json').read_text())['status'] == 'ready'


def test_pending_publication_never_retries_create(tmp_path):
    final_files(tmp_path)
    (tmp_path/'.publish-state.json').write_text(json.dumps({'ref': 'owner/game', 'version': 1, 'status': 'pending'}))
    api = Mock()
    backend = Backend('owner', client=client(datasets=api), ready_timeout=0)
    backend.inspect_dataset = Mock(return_value=None)
    with pytest.raises(OutcomeUnknown):
        backend.publish(tmp_path, 'owner/game')
    api.create_dataset.assert_not_called()
    api.create_dataset_version.assert_not_called()


def test_ready_with_missing_file_is_not_accepted():
    backend = Backend(client=client(), ready_timeout=0)
    backend.inspect_dataset = Mock(return_value=ready(files=['metadata.json', 'summary.json']))
    with pytest.raises(OutcomeUnknown):
        backend._wait_ready('owner/game', 1, FINAL_FILES)


def test_only_404_means_dataset_absent():
    api = Mock()
    backend = Backend(client=client(datasets=api))
    api.get_dataset.side_effect = http_error(403)
    api.list_datasets.side_effect = http_error(403)
    with pytest.raises(requests.HTTPError):
        backend.inspect_dataset('owner/game')
    api.get_dataset.side_effect = http_error(404)
    assert backend.inspect_dataset('owner/game') is None


def test_dataset_files_are_paginated_at_specific_version():
    api = Mock()
    api.get_dataset.return_value = N(current_version_number=3, is_private=False)
    api.get_dataset_status.return_value = N(status=N(name='READY'))
    api.list_dataset_files.side_effect = [N(dataset_files=[N(name='metadata.json')], next_page_token='next', error_message=''),
                                        N(dataset_files=[N(name='summary.json'), N(name='orderbook.jsonl')], next_page_token='', error_message='')]
    backend = Backend(client=client(datasets=api))
    backend.read_json = Mock(return_value={'stop_reason':'six_hour_deadline'})
    result = backend.inspect_dataset('owner/game')
    assert result['files'] == sorted(FINAL_FILES)
    assert all(c.args[0].dataset_version_number == 3 for c in api.list_dataset_files.call_args_list)
    assert api.list_dataset_files.call_args_list[1].args[0].page_token == 'next'


def test_unchanged_catalog_has_no_write(tmp_path):
    backend = Backend('owner', client=client())
    backend.inspect_dataset = Mock(return_value=ready('owner/nba-polymarket-orderbook', files=['catalog.json']))
    rows = [{'slug': 'nba-game', 'version': 1}]
    backend.read_json = Mock(return_value=rows)
    backend._publish = Mock()
    assert backend.catalog(rows, tmp_path)['changed'] is False
    backend._publish.assert_not_called()


def test_registered_older_version_is_counted_when_latest_is_complete():
    api = Mock()
    api.list_kernels.return_value = N(kernels=[N(ref='owner/other', current_version_number=2,
                                                 enable_gpu=False, enable_tpu=False)], next_page_token='')
    backend = Backend('owner', client=client(kernels=api), known_sessions=[{'ref':'owner/other','version':1}])
    backend.snapshot = Mock(side_effect=lambda ref,version=None: {'ref':ref,'version':version or 2,'status':'RUNNING' if version==1 else 'COMPLETE',
                                                           'enable_gpu':False,'enable_tpu':False})
    assert [x['version'] for x in backend.list_active()] == [1]


def test_existing_final_with_different_summary_is_not_claimed_as_this_upload(tmp_path):
    final_files(tmp_path)
    backend = Backend(client=client(), ready_timeout=0)
    backend.inspect_dataset = Mock(return_value=ready())
    backend.read_json = Mock(side_effect=[{}, {'stop_reason':'different'}])
    with pytest.raises(RuntimeError, match='existing_final_dataset_differs'):
        backend.publish(tmp_path, 'owner/game')


def test_catalog_reconciles_old_pending_before_writing_new_rows(tmp_path):
    (tmp_path/'.publish-state.json').write_text(json.dumps({'ref':'owner/nba-polymarket-orderbook','version':2,'status':'pending'}))
    backend = Backend('owner', client=client(), ready_timeout=0)
    backend.inspect_dataset = Mock(return_value=ready('owner/nba-polymarket-orderbook', 2, ['catalog.json']))
    backend.read_json = Mock(return_value=[{'slug':'old'}])
    backend._publish = Mock(return_value=ready('owner/nba-polymarket-orderbook',3,['catalog.json']))
    assert backend.catalog([{'slug':'new'}],tmp_path)['version']==3
    assert json.loads((tmp_path/'catalog.json').read_text())==[{'slug':'new'}]
    assert json.loads((tmp_path/'.publish-state.json').read_text())['status']=='ready'


def test_kernel_on_second_page_is_not_created():
    api = Mock()
    api.list_kernels.side_effect = [N(kernels=[N(ref='owner/other')], next_page_token='p2'),
                                    N(kernels=[N(ref='owner/nba-batch-batch')], next_page_token='')]
    backend = Backend('owner', client=client(kernels=api))
    backend.snapshot = Mock(return_value={'batch_id':'batch','status':'RUNNING'})
    assert backend.submit({'id':'batch'},Path('/unused'))['status']=='RUNNING'
    api.save_kernel.assert_not_called()
    assert api.list_kernels.call_args_list[1].args[0].page_token=='p2'


@pytest.mark.parametrize('page_one', [True, False])
def test_kernel_listing_error_prevents_creation(page_one):
    api = Mock()
    api.list_kernels.side_effect = [http_error(403)] if page_one else [N(kernels=[],next_page_token='p2'),http_error(403)]
    backend = Backend('owner', client=client(kernels=api))
    with pytest.raises(SubmissionNotAttempted):
        backend.submit({'id':'batch'},Path('/unused'))
    api.save_kernel.assert_not_called()


def test_dataset_403_absence_requires_complete_own_listing():
    api = Mock()
    api.get_dataset.side_effect = http_error(403)
    api.list_datasets.side_effect = [N(datasets=[N(ref='owner/other')],next_page_token='p2'),
                                     N(datasets=[],next_page_token='')]
    backend = Backend('owner', client=client(datasets=api))
    assert backend.inspect_dataset('owner/game') is None
    assert api.list_datasets.call_args_list[1].args[0].page_token=='p2'


def test_dataset_403_existing_on_second_page_remains_permission_error():
    api = Mock()
    api.get_dataset.side_effect = http_error(403)
    api.list_datasets.side_effect = [N(datasets=[],next_page_token='p2'),
                                     N(datasets=[N(ref='owner/game')],next_page_token='')]
    with pytest.raises(requests.HTTPError):
        Backend('owner', client=client(datasets=api)).inspect_dataset('owner/game')


def test_dataset_listing_failure_is_not_absence():
    api = Mock()
    api.get_dataset.side_effect = http_error(403)
    api.list_datasets.side_effect = [N(datasets=[],next_page_token='p2'),http_error(503)]
    with pytest.raises(requests.HTTPError):
        Backend('owner',client=client(datasets=api)).inspect_dataset('owner/game')


def test_other_owners_403_is_not_treated_as_own_absence():
    api = Mock()
    api.get_dataset.side_effect = http_error(403)
    with pytest.raises(requests.HTTPError):
        Backend('owner',client=client(datasets=api)).inspect_dataset('another/game')
    api.list_datasets.assert_not_called()


def test_sparse_kernel_listing_uses_actual_latest_version_and_resources():
    from kagglesdk.kernels.types.kernels_api_service import ApiKernelMetadata
    sparse = ApiKernelMetadata()
    sparse.ref = 'owner/other'
    assert sparse.current_version_number == 0
    api = Mock()
    api.list_kernels.return_value = N(kernels=[sparse],next_page_token='')
    api.get_kernel.return_value = N(metadata=N(current_version_number=7,enable_gpu=False,enable_tpu=False),
                                   blob=N(source='{}'))
    api.get_kernel_session_status.return_value = N(status=N(name='RUNNING'))
    active = Backend('owner',client=client(kernels=api)).list_active()
    assert [x['version'] for x in active]==[7]
    assert api.get_kernel.call_args.args[0].version_label == ''
    assert api.get_kernel_session_status.call_args.args[0].version_label == 'v7'


def test_final_upload_retains_local_gzip_and_accepts_remote_jsonl(tmp_path):
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile
    final_files(tmp_path)
    api = Mock()
    api.create_dataset.return_value = N(error='')
    backend = Backend('owner',client=client(datasets=api),ready_timeout=0)
    backend.inspect_dataset = Mock(side_effect=[None, ready()])
    backend._upload = Mock(return_value=ApiDatasetNewFile())
    result = backend.publish(tmp_path,'owner/game')
    assert [call.args[0].name for call in backend._upload.call_args_list] == ['orderbook.jsonl.gz','metadata.json','summary.json']
    assert result['files'] == ['orderbook.jsonl','metadata.json','summary.json']
    assert (tmp_path/'orderbook.jsonl.gz').exists()
    assert not (tmp_path/'orderbook.jsonl').exists()
    assert api.create_dataset.call_count==1
    api.create_dataset_version.assert_not_called()


def test_existing_remote_jsonl_final_is_reused_without_upload_or_version(tmp_path):
    final_files(tmp_path)
    api = Mock()
    backend = Backend('owner',client=client(datasets=api),ready_timeout=0)
    backend.inspect_dataset = Mock(return_value=ready())
    backend.read_json = Mock(return_value={})
    backend._upload = Mock()
    result=backend.publish(tmp_path,'owner/game')
    assert result['version']==1
    backend._upload.assert_not_called()
    api.create_dataset.assert_not_called()
    api.create_dataset_version.assert_not_called()


def test_pending_gzip_upload_reconciles_expanded_v1_without_any_remote_write(tmp_path):
    final_files(tmp_path)
    (tmp_path/'.publish-state.json').write_text(json.dumps({'ref':'owner/game','version':1,'status':'pending'}))
    api = Mock()
    backend = Backend('owner',client=client(datasets=api),ready_timeout=0)
    backend.inspect_dataset = Mock(return_value=ready())
    backend._upload = Mock()
    assert backend.publish(tmp_path,'owner/game')['version']==1
    assert json.loads((tmp_path/'.publish-state.json').read_text())['status']=='ready'
    backend._upload.assert_not_called()
    api.create_dataset.assert_not_called()
    api.create_dataset_version.assert_not_called()


def test_catalog_upload_and_remote_names_are_unchanged(tmp_path):
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile
    api=Mock()
    api.create_dataset.return_value=N(error='')
    backend=Backend('owner',client=client(datasets=api),ready_timeout=0)
    backend.inspect_dataset=Mock(side_effect=[None,None,ready('owner/nba-polymarket-orderbook',files=['catalog.json'])])
    backend._upload=Mock(return_value=ApiDatasetNewFile())
    result=backend.catalog([{'slug':'game'}],tmp_path)
    assert result['files']==['catalog.json']
    assert [call.args[0].name for call in backend._upload.call_args_list]==['catalog.json']


def test_rejected_dataset_create_can_retry_after_remote_recheck(tmp_path):
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile
    final_files(tmp_path)
    api = Mock()
    backend = Backend('owner', client=client(datasets=api), ready_timeout=0)
    backend.inspect_dataset = Mock(side_effect=[None, None, ready()])
    backend._upload = Mock(return_value=ApiDatasetNewFile())
    def create(request):
        if api.create_dataset.call_count == 1:
            return N(error='rejected')
        assert backend.inspect_dataset.call_count == 2
        return N(error='')
    api.create_dataset.side_effect = create
    with pytest.raises(RuntimeError, match='dataset_write_rejected'):
        backend.publish(tmp_path, 'owner/game')
    assert json.loads((tmp_path/'.publish-state.json').read_text())['status'] == 'rejected'
    assert backend.inspect_dataset.call_count == 1
    assert api.create_dataset.call_count == 1
    result = backend.publish(tmp_path, 'owner/game')
    assert result['version'] == 1
    assert api.create_dataset.call_count == 2
    api.create_dataset_version.assert_not_called()
    assert json.loads((tmp_path/'.publish-state.json').read_text())['status'] == 'ready'


def test_rejected_catalog_version_recovers_on_next_scan(tmp_path):
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile
    ref = 'owner/nba-polymarket-orderbook'
    v1 = ready(ref, 1, ['catalog.json'])
    v2 = ready(ref, 2, ['catalog.json'])
    api = Mock()
    backend = Backend('owner', client=client(datasets=api), ready_timeout=0)
    backend.inspect_dataset = Mock(side_effect=[v1, v1, v1, v1, v2])
    backend.read_json = Mock(return_value=[{'slug':'old'}])
    backend._upload = Mock(return_value=ApiDatasetNewFile())
    def create_version(request):
        if api.create_dataset_version.call_count == 1:
            return N(error='rejected')
        assert backend.inspect_dataset.call_count == 4
        return N(error='')
    api.create_dataset_version.side_effect = create_version
    with pytest.raises(RuntimeError, match='dataset_write_rejected'):
        backend.catalog([{'slug':'new'}], tmp_path)
    assert json.loads((tmp_path/'.publish-state.json').read_text()) == {'ref':ref, 'version':2, 'status':'rejected'}
    assert api.create_dataset_version.call_count == 1
    assert backend.catalog([{'slug':'new'}], tmp_path)['version'] == 2
    assert api.create_dataset_version.call_count == 2
    api.create_dataset.assert_not_called()


@pytest.mark.parametrize('catalog', [False, True])
def test_unknown_publication_remains_pending_and_does_not_repeat_write(tmp_path, catalog):
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile
    final_files(tmp_path)
    api = Mock()
    api.create_dataset.side_effect = requests.Timeout()
    api.create_dataset_version.side_effect = requests.Timeout()
    backend = Backend('owner', client=client(datasets=api), ready_timeout=0)
    existing = ready('owner/nba-polymarket-orderbook', 1, ['catalog.json']) if catalog else None
    backend.inspect_dataset = Mock(return_value=existing)
    backend.read_json = Mock(return_value=[{'slug':'old'}])
    backend._upload = Mock(return_value=ApiDatasetNewFile())
    def publish():
        return backend.catalog([{'slug':'new'}], tmp_path) if catalog else backend.publish(tmp_path,'owner/game')
    with pytest.raises(OutcomeUnknown):
        publish()
    assert json.loads((tmp_path/'.publish-state.json').read_text())['status'] == 'pending'
    uploads = backend._upload.call_count
    with pytest.raises(OutcomeUnknown):
        publish()
    assert json.loads((tmp_path/'.publish-state.json').read_text())['status'] == 'pending'
    assert backend._upload.call_count == uploads
    assert api.create_dataset_version.call_count == int(catalog)
    assert api.create_dataset.call_count == int(not catalog)


@pytest.mark.parametrize('failure', [requests.Timeout(), ValueError('malformed read response')])
def test_submission_read_failure_can_recover_with_one_actual_write(tmp_path, failure):
    from kagglesdk.kernels.types.kernels_api_service import ApiSaveKernelResponse
    api = Mock()
    api.list_kernels.side_effect = [failure, N(kernels=[], next_page_token='')]
    response = ApiSaveKernelResponse()
    response.version_number = 1
    api.save_kernel.return_value = response
    backend = Backend('owner', client=client(kernels=api))
    (tmp_path/'src/nba_orderbook').mkdir(parents=True)
    (tmp_path/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    with pytest.raises(SubmissionNotAttempted):
        backend.submit({'id':'batch'}, tmp_path)
    api.save_kernel.assert_not_called()
    result = backend.submit({'id':'batch'}, tmp_path)
    assert result['ref'] == 'owner/nba-batch-batch'
    assert result['version'] == 1
    assert api.save_kernel.call_count == 1


@pytest.mark.parametrize('failure', [requests.Timeout(), ValueError('malformed snapshot')])
def test_existing_kernel_read_failure_is_unknown_not_retryable(failure):
    api = Mock()
    api.list_kernels.return_value = N(kernels=[N(ref='owner/nba-batch-batch')], next_page_token='')
    backend = Backend('owner', client=client(kernels=api))
    backend.snapshot = Mock(side_effect=failure)
    with pytest.raises(OutcomeUnknown):
        backend.submit({'id':'batch'}, Path('/unused'))
    api.save_kernel.assert_not_called()


@pytest.mark.parametrize('failure', [requests.Timeout(), ValueError('malformed write response')])
def test_write_failure_then_failed_recheck_is_always_unknown(tmp_path, failure):
    api = Mock()
    api.list_kernels.return_value = N(kernels=[], next_page_token='')
    api.save_kernel.side_effect = failure
    backend = Backend('owner', client=client(kernels=api))
    backend.snapshot = Mock(side_effect=ValueError('malformed recheck response'))
    (tmp_path/'src/nba_orderbook').mkdir(parents=True)
    (tmp_path/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    with pytest.raises(OutcomeUnknown):
        backend.submit({'id':'batch'}, tmp_path)
    assert api.save_kernel.call_count == 1
    assert backend.snapshot.call_count == 1


def test_unexpected_write_response_recovers_same_instance_without_resubmit(tmp_path):
    api = Mock()
    api.list_kernels.return_value = N(kernels=[], next_page_token='')
    api.save_kernel.return_value = None
    backend = Backend('owner', client=client(kernels=api))
    backend.snapshot = Mock(return_value={'batch_id':'batch', 'ref':'owner/nba-batch-batch', 'version':1, 'status':'RUNNING'})
    (tmp_path/'src/nba_orderbook').mkdir(parents=True)
    (tmp_path/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    assert backend.submit({'id':'batch'}, tmp_path)['status']=='RUNNING'
    assert api.save_kernel.call_count==1
