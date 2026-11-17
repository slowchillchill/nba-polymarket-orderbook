"""Kaggle 官方 SDK 适配；不重复提交结果未知的写操作。"""
from __future__ import annotations

import base64
import io
import json
from pathlib import Path
import threading
import time
import zipfile

import requests
from kagglesdk import KaggleClient
from kagglesdk.blobs.types.blob_api_service import ApiStartBlobUploadRequest, ApiBlobType
from kagglesdk.datasets.types.dataset_api_service import (
    ApiCreateDatasetRequest, ApiCreateDatasetVersionRequest,
    ApiCreateDatasetVersionRequestBody, ApiDatasetNewFile, ApiDownloadDatasetRequest,
    ApiGetDatasetRequest, ApiGetDatasetStatusRequest, ApiListDatasetFilesRequest,
    ApiListDatasetsRequest,
)
from kagglesdk.datasets.types.dataset_enums import DatasetSelectionGroup
from kagglesdk.kernels.types.kernels_api_service import (
    ApiGetKernelRequest, ApiGetKernelSessionStatusRequest, ApiListKernelsRequest,
    ApiSaveKernelRequest,
)
from kagglesdk.kernels.types.kernels_enums import KernelExecutionType, KernelsListViewType

ACTIVE = {'QUEUED', 'RUNNING', 'CANCEL_REQUESTED'}
# Kaggle 在发布时展开 gzip；本地采集和上传仍保留压缩表示。
UPLOAD_FILES = ('orderbook.jsonl.gz', 'metadata.json', 'summary.json')
FINAL_FILES = ('orderbook.jsonl', 'metadata.json', 'summary.json')
MARKER = '# nba_batch_id: '


class SubmissionNotAttempted(RuntimeError):
    """首次只读查询失败，尚未发送写请求；重试必须保留同一批次身份。"""


class SubmissionRejected(RuntimeError):
    """写请求被明确拒绝且回查确认未创建；重试必须保留同一批次身份。"""


class OutcomeUnknown(RuntimeError):
    """远端写入结果尚未确认；调用者必须保留原实例或发布状态。"""


class DatasetProcessingFailed(RuntimeError):
    """已确认指定 Dataset 版本处理失败。"""

    def __init__(self, ref, version):
        super().__init__('dataset_processing_failed')
        self.ref = ref
        self.version = version


def _request(cls, **values):
    request = cls()
    for key, value in values.items():
        setattr(request, key, value)
    return request


def _save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding='utf-8')
    temporary.replace(path)


def notebook_source(batch: dict, package_root: Path) -> str:
    """只打包源码和锁定依赖；凭据由 Kaggle 运行环境注入。"""
    root = Path(package_root)
    files = {str(p.relative_to(root / 'src')): p.read_text(encoding='utf-8')
             for p in sorted((root / 'src' / 'nba_orderbook').glob('*.py'))}
    requirements = (root / 'requirements.txt').read_text(encoding='utf-8')
    payload = base64.b64encode(json.dumps({'files': files, 'requirements': requirements,
                                         'batch': batch}).encode()).decode()
    code = f'''{MARKER}{batch['id']}
import base64, json, os, pathlib, subprocess, sys
payload = json.loads(base64.b64decode({payload!r}))
root = pathlib.Path('/kaggle/working/nba-app')
root.mkdir(parents=True, exist_ok=True)
for name, source in payload['files'].items():
    target = root / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source, encoding='utf-8')
(root / 'requirements.txt').write_text(payload['requirements'], encoding='utf-8')
(root / 'batch.json').write_text(json.dumps(payload['batch']), encoding='utf-8')
subprocess.run([sys.executable, '-m', 'pip', 'install', '--quiet', 'uv==0.11.7'], check=True)
subprocess.run([sys.executable, '-m', 'uv', 'venv', '--python', '3.12', str(root / '.venv')], check=True)
python = str(root / '.venv/bin/python')
subprocess.run([sys.executable, '-m', 'uv', 'pip', 'install', '--python', python, '-r', str(root / 'requirements.txt')], check=True)
env = dict(os.environ, PYTHONPATH=str(root))
identity_source = subprocess.check_output([python, '-c', 'from kagglesdk.kaggle_env import get_access_token_from_env; print(get_access_token_from_env()[1])'], env=env, text=True).strip()
print(json.dumps({{'identity_source': identity_source}}), flush=True)
subprocess.run([python, '-m', 'nba_orderbook.cli', 'collect', '--batch', str(root / 'batch.json'), '--output', '/kaggle/working/results'], env=env, check=True)
if payload['batch'].get('qa_seconds'):
    import gzip
    report = {{'identity_source': identity_source, 'files': []}}
    for path in pathlib.Path('/kaggle/working/results').glob('*/orderbook.jsonl.gz'):
        with gzip.open(path, 'rt', encoding='utf-8') as stream:
            count = sum(1 for line in stream if json.loads(line) is not None)
        metadata = json.loads(path.with_name('metadata.json').read_text())
        summary = json.loads(path.with_name('summary.json').read_text())
        report['files'].append({{'slug': path.parent.name, 'gzip_readable': True, 'records': count, 'summary_messages': summary.get('message_count')}})
    pathlib.Path('/kaggle/working/qa-report.json').write_text(json.dumps(report))
    print(json.dumps(report), flush=True)
'''
    return json.dumps({'nbformat': 4, 'nbformat_minor': 5,
                       'metadata': {'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'}},
                       'cells': [{'cell_type': 'code', 'metadata': {}, 'execution_count': None,
                                  'outputs': [], 'source': code}]})


class Backend:
    def __init__(self, owner='qiuqiuslowchillchill', *, client=None, known_sessions=(),
                 ready_timeout=600, poll_seconds=10):
        self.owner = owner
        self._provided_client = client
        self._local = threading.local()
        self.known_sessions = list(known_sessions)
        self.ready_timeout = ready_timeout
        self.poll_seconds = poll_seconds

    @property
    def client(self):
        if self._provided_client is not None:
            return self._provided_client
        if not hasattr(self._local, 'client'):
            client = KaggleClient(verbose=False, user_agent='nba-orderbook/0.1')
            http = client.http_client()
            http._init_session()
            # SDK 0.1.38 没有 timeout 参数；只为当前会话设置有界传输。
            send = http._session.send
            def bounded_send(request, **kwargs):
                kwargs['timeout'] = (15, 90)
                return send(request, **kwargs)
            http._session.send = bounded_send
            self._local.client = client
        return self._local.client

    def snapshot(self, ref, version=None):
        owner, slug = ref.split('/', 1)
        request = _request(ApiGetKernelRequest, user_name=owner, kernel_slug=slug)
        if version is not None:
            request.version_label = f'v{version}'
        response = self.client.kernels.kernels_api_client.get_kernel(request)
        selected = version or response.metadata.current_version_number
        status = self.client.kernels.kernels_api_client.get_kernel_session_status(
            _request(ApiGetKernelSessionStatusRequest, user_name=owner, kernel_slug=slug,
                     version_label=f'v{selected}'))
        batch_id = None
        try:
            notebook = json.loads(response.blob.source)
            for cell in notebook.get('cells', []):
                source = cell.get('source', '')
                if isinstance(source, list):
                    source = ''.join(source)
                for line in source.splitlines():
                    if line.startswith(MARKER):
                        batch_id = line[len(MARKER):]
        except (ValueError, AttributeError):
            pass
        return {'ref': ref, 'version': selected, 'status': status.status.name,
                'batch_id': batch_id, 'enable_gpu': response.metadata.enable_gpu,
                'enable_tpu': response.metadata.enable_tpu}

    def _own_kernels(self):
        """完整读取自己的 Notebook；列表失败不能证明某个引用不存在。"""
        kernels, token = [], ''
        while True:
            request = _request(ApiListKernelsRequest, group=KernelsListViewType.PROFILE,
                               user=self.owner, language='all', kernel_type='all',
                               output_type='all', page_size=100)
            if token:
                request.page_token = token
            response = self.client.kernels.kernels_api_client.list_kernels(request)
            kernels.extend(response.kernels)
            token = response.next_page_token
            if not token:
                return kernels

    def _own_dataset_absent(self, ref):
        if ref.split('/', 1)[0] != self.owner:
            return False
        token = ''
        while True:
            request = _request(ApiListDatasetsRequest,
                               group=DatasetSelectionGroup.DATASET_SELECTION_GROUP_MY,
                               page_size=100)
            if token:
                request.page_token = token
            response = self.client.datasets.dataset_api_client.list_datasets(request)
            if any(dataset.ref == ref for dataset in response.datasets):
                return False
            token = response.next_page_token
            if not token:
                return True

    def submit(self, batch, package_root, *, qa=False, timeout_seconds=43200):
        slug = ('nba-qa-' if qa else 'nba-batch-') + batch['id']
        ref = f'{self.owner}/{slug}'
        # Kaggle 对缺失引用也可能返回 403；不靠 GetKernel 的状态码判不存在。
        try:
            kernels = self._own_kernels()
        except Exception:
            raise SubmissionNotAttempted('kernel_listing_failed_before_submission') from None
        if any(kernel.ref == ref for kernel in kernels):
            try:
                existing = self.snapshot(ref)
            except Exception:
                raise OutcomeUnknown('existing_kernel_status_unknown') from None
            if existing['batch_id'] == batch['id']:
                return existing
            raise OutcomeUnknown('kernel_exists_with_other_batch')
        request = _request(ApiSaveKernelRequest, slug=ref, new_title=slug,
                           text=notebook_source(batch, package_root), language='python',
                           kernel_type='notebook', is_private=True, enable_gpu=False,
                           enable_tpu=False, enable_internet=True, dataset_data_sources=[],
                           competition_data_sources=[], kernel_data_sources=[], model_data_sources=[],
                           kernel_execution_type=KernelExecutionType.SAVE_AND_RUN_ALL,
                           session_timeout_seconds=min(timeout_seconds, 3600) if qa else timeout_seconds)
        try:
            result = self.client.kernels.kernels_api_client.save_kernel(request)
            # 官方 CLI 以 error 判断拒绝；invalid_* 本身不证明未创建。
            rejected = bool(result.error) and result.version_number == 0
            if not rejected:
                if result.error or result.invalid_tags or result.invalid_dataset_sources or result.invalid_kernel_sources or result.invalid_competition_sources or result.invalid_model_sources:
                    raise RuntimeError('kernel_submission_needs_reconciliation')
                return {'ref': ref, 'version': result.version_number, 'status': 'QUEUED', 'batch_id': batch['id']}
        except Exception:
            # SaveKernel 一旦开始调用，异常不能证明未写入；只回查同一 ref。
            return self._submitted_or_unknown(ref, batch['id'])
        try:
            exists = any(kernel.ref == ref for kernel in self._own_kernels())
        except Exception:
            raise OutcomeUnknown('rejected_kernel_presence_unknown') from None
        if not exists:
            raise SubmissionRejected('kernel_submission_rejected_without_creation')
        return self._submitted_or_unknown(ref, batch['id'])

    def _submitted_or_unknown(self, ref, batch_id):
        try:
            observed = self.snapshot(ref)
            if observed['batch_id'] != batch_id:
                raise OutcomeUnknown('kernel_submission_outcome_unknown')
        except Exception:
            raise OutcomeUnknown('kernel_submission_outcome_unknown') from None
        return observed

    def list_active(self):
        """已观测占用：账号最新版本及显式登记版本；不是 CPU 配额证明。"""
        observed = {}
        for kernel in self._own_kernels():
            # ListKernels 的稀疏结果可能把版本/资源字段留为默认值。
            snapshot = self.snapshot(kernel.ref)
            observed[(kernel.ref, snapshot['version'])] = snapshot
        for item in self.known_sessions:
            key = (item['ref'], item['version'])
            if key not in observed:
                observed[key] = self.snapshot(*key)
        active = []
        for snapshot in observed.values():
            if snapshot['status'] in ACTIVE and not snapshot['enable_gpu'] and not snapshot['enable_tpu']:
                snapshot['coverage'] = 'latest_and_registered_versions'
                active.append(snapshot)
        return active

    def inspect_dataset(self, ref):
        owner, slug = ref.split('/', 1)
        api = self.client.datasets.dataset_api_client
        try:
            dataset = api.get_dataset(_request(ApiGetDatasetRequest, owner_slug=owner, dataset_slug=slug))
        except requests.HTTPError as exc:
            if exc.response is not None:
                if exc.response.status_code == 404:
                    return None
                if exc.response.status_code == 403 and self._own_dataset_absent(ref):
                    return None
            raise
        status = api.get_dataset_status(_request(ApiGetDatasetStatusRequest, owner_slug=owner, dataset_slug=slug))
        files, token = [], ''
        while True:
            request = _request(ApiListDatasetFilesRequest, owner_slug=owner, dataset_slug=slug,
                               dataset_version_number=dataset.current_version_number, page_size=100)
            if token:
                request.page_token = token
            result = api.list_dataset_files(request)
            if result.error_message:
                raise RuntimeError('dataset_file_listing_failed')
            files.extend(file.name for file in result.dataset_files)
            token = result.next_page_token
            if not token:
                break
        result = {'ref': ref, 'version': dataset.current_version_number,
                  'status': status.status.name, 'files': sorted(files),
                  'url': f'https://www.kaggle.com/datasets/{ref}', 'is_private': dataset.is_private}
        if result['status'] == 'READY' and set(FINAL_FILES).issubset(files):
            result['summary'] = self.read_json(ref, 'summary.json', result['version'])
        return result

    def read_json(self, ref, name, version=None):
        """只读取目录/摘要等小文件；不下载整场原始推送。"""
        owner, slug = ref.split('/', 1)
        request = _request(ApiDownloadDatasetRequest, owner_slug=owner, dataset_slug=slug,
                           file_name=name, raw=True)
        if version is not None:
            request.dataset_version_number = version
        response = self.client.datasets.dataset_api_client.download_dataset(request)
        try:
            content = response.content
        finally:
            response.close()
        if content[:2] == b'PK':
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                content = archive.read(name)
        return json.loads(content)

    def _upload(self, path):
        request = _request(ApiStartBlobUploadRequest, type=ApiBlobType.DATASET,
                           name=path.name, content_length=path.stat().st_size,
                           last_modified_epoch_seconds=int(path.stat().st_mtime))
        response = self.client.blobs.blob_api_client.start_blob_upload(request)
        try:
            with path.open('rb') as stream:
                uploaded = requests.put(response.create_url, data=stream, timeout=(15, 300))
            uploaded.raise_for_status()
        except requests.RequestException:
            # 官方上传协议支持同一 URL 的 Content-Range 状态查询。
            # 查询本身不重复创建 Dataset，也不输出带签名 URL。
            try:
                observed = requests.put(response.create_url, data=b'',
                                        headers={'Content-Length': '0', 'Content-Range': f'bytes */{path.stat().st_size}'},
                                        timeout=(15, 90))
            except requests.RequestException:
                raise OutcomeUnknown('blob_upload_outcome_unknown') from None
            if observed.status_code not in (200, 201):
                raise OutcomeUnknown('blob_upload_incomplete') from None
        return _request(ApiDatasetNewFile, token=response.token)

    def _wait_ready(self, ref, expected_version, names):
        deadline = time.monotonic() + self.ready_timeout
        while True:
            try:
                observed = self.inspect_dataset(ref)
            except requests.RequestException:
                observed = None
            if observed and observed['version'] >= expected_version:
                if observed['status'] == 'FAILED' and observed['version'] == expected_version:
                    raise DatasetProcessingFailed(ref, expected_version)
                if observed['status'] == 'DELETED':
                    raise RuntimeError('dataset_processing_failed')
                if observed['status'] == 'READY' and set(names).issubset(observed['files']):
                    return observed
            if time.monotonic() >= deadline:
                raise OutcomeUnknown('dataset_ready_not_confirmed')
            time.sleep(self.poll_seconds)

    def _publish(self, directory, ref, names, *, public, new_version=False, remote_names=None,
                 failed_catalog_version=None):
        directory = Path(directory)
        remote_names = names if remote_names is None else remote_names
        prior = self.inspect_dataset(ref)
        marker = directory / '.publish-state.json'
        pending = json.loads(marker.read_text()) if marker.exists() else None
        if pending and pending['ref'] != ref:
            raise RuntimeError('publication_directory_belongs_to_other_dataset')
        if pending and pending['status'] == 'pending':
            result = self._wait_ready(ref, pending['version'], remote_names)
            _save(marker, {'ref': ref, 'version': result['version'], 'status': 'ready'})
            return result
        if failed_catalog_version is not None:
            # 仅目录允许从确知失败版本恢复；上传前重新确认当前远端版本。
            if prior and prior['status'] == 'READY' and prior['version'] >= failed_catalog_version:
                local = json.loads((directory / 'catalog.json').read_text())
                if self.read_json(ref, 'catalog.json', prior['version']) == local:
                    _save(marker, {'ref': ref, 'version': prior['version'], 'status': 'ready'})
                    return {**prior, 'changed': False}
            elif not (prior and prior['status'] == 'FAILED' and prior['version'] == failed_catalog_version):
                raise OutcomeUnknown('failed_catalog_current_version_unknown')
        if prior and not new_version:
            result = self._wait_ready(ref, prior['version'], remote_names)
            # 已有最终版不能被本次不同的数据冒充为成功发布。
            for name in ('metadata.json', 'summary.json'):
                local = json.loads((directory / name).read_text())
                remote = self.read_json(ref, name, result['version'])
                if remote != local:
                    raise RuntimeError('existing_final_dataset_differs')
            return result
        files = [self._upload(directory / name) for name in names]
        owner, slug = ref.split('/', 1)
        expected = prior['version'] + 1 if prior else 1
        _save(marker, {'ref': ref, 'version': expected, 'status': 'pending'})
        api = self.client.datasets.dataset_api_client
        try:
            if prior:
                body = _request(ApiCreateDatasetVersionRequestBody, version_notes='更新 NBA 比赛目录',
                                delete_old_versions=False, files=files)
                response = api.create_dataset_version(_request(ApiCreateDatasetVersionRequest,
                                      owner_slug=owner, dataset_slug=slug, body=body))
            else:
                response = api.create_dataset(_request(ApiCreateDatasetRequest, owner_slug=owner,
                                  slug=slug, title=slug, license_name='CC0-1.0', is_private=not public,
                                  files=files, description='NBA Polymarket 全场胜负市场订单簿与采集说明。'))
            if response.error:
                # 明确拒绝没有未知副作用；下次调用仍先回查远端，再尝试发布。
                _save(marker, {'ref': ref, 'version': expected, 'status': 'rejected'})
                raise RuntimeError('dataset_write_rejected')
        except requests.RequestException:
            # 保留 pending；只回查一次写入的实际结果。
            pass
        result = self._wait_ready(ref, expected, remote_names)
        _save(marker, {'ref': ref, 'version': result['version'], 'status': 'ready'})
        return result

    def publish(self, directory, dataset_ref, *, public=True):
        return self._publish(directory, dataset_ref, UPLOAD_FILES, public=public, remote_names=FINAL_FILES)

    def catalog(self, rows, directory):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        rows = sorted(rows, key=lambda row: row['slug'])
        ref = f'{self.owner}/nba-polymarket-orderbook'
        marker = directory / '.publish-state.json'
        pending = json.loads(marker.read_text()) if marker.exists() else None
        if pending and pending['ref'] != ref:
            raise RuntimeError('publication_directory_belongs_to_other_dataset')
        try:
            if pending and pending['status'] == 'pending':
                reconciled = self._wait_ready(ref, pending['version'], ['catalog.json'])
                _save(marker, {'ref': ref, 'version': reconciled['version'], 'status': 'ready'})
            failed_version = pending['version'] if pending and pending['status'] == 'failed' else None
            prior = self.inspect_dataset(ref)
            if prior and prior['status'] == 'READY':
                if failed_version is not None and prior['version'] < failed_version:
                    raise OutcomeUnknown('failed_catalog_current_version_unknown')
                if self.read_json(ref, 'catalog.json', prior['version']) == rows:
                    if failed_version is not None:
                        _save(marker, {'ref': ref, 'version': prior['version'], 'status': 'ready'})
                    return {**prior, 'changed': False}
            _save(directory / 'catalog.json', rows)
            result = self._publish(directory, ref, ['catalog.json'], public=True, new_version=bool(prior),
                                   failed_catalog_version=failed_version)
            return {**result, 'changed': result.get('changed', True)}
        except DatasetProcessingFailed as exc:
            state = json.loads(marker.read_text()) if marker.exists() else None
            if state and state['ref'] == exc.ref == ref and state['status'] == 'pending' and state['version'] == exc.version:
                _save(marker, {'ref': ref, 'version': exc.version, 'status': 'failed'})
            raise
