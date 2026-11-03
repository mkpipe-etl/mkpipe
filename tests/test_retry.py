from __future__ import annotations

import pytest

from mkpipe import api
from mkpipe.exceptions import ConfigError
from mkpipe.models import ExtractResult, TableConfig


class _RecordingLogger:
    def __init__(self):
        self.calls = []

    def _record(self, level, message):
        self.calls.append((level, message))

    def debug(self, message):
        self._record('debug', message)

    def info(self, message):
        self._record('info', message)

    def warning(self, message):
        self._record('warning', message)

    def error(self, message):
        self._record('error', message)

    def exception(self, message):
        self._record('exception', message)

    def critical(self, message):
        self._record('critical', message)

    def by_level(self, level):
        return [m for lvl, m in self.calls if lvl == level]

    def statuses(self, level):
        return [m.get('status') for m in self.by_level(level)]


class _FakeBackend:
    def __init__(self, initial_status=None):
        self.initial_status = initial_status
        self.status_updates = []
        self.status_reads = 0

    def get_table_status(self, pipeline_name, table_name):
        self.status_reads += 1
        return self.initial_status

    def get_last_point(self, pipeline_name, table_name):
        return None

    def manifest_table_update(self, pipeline_name, table_name, status, **kwargs):
        self.status_updates.append(status)

    def statuses(self):
        return [u for u in self.status_updates if u in ('extracting', 'loading', 'failed')]


class _FakeExtractor:
    """Raises the configured exceptions in order, then succeeds."""

    def __init__(self, errors=()):
        self.errors = list(errors)
        self.calls = 0

    def extract(self, table, spark, last_point=None):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return ExtractResult(df=None, write_mode='overwrite', last_point_value=None)


class _FakeLoader:
    def __init__(self):
        self.calls = 0

    def load(self, table, data, spark):
        self.calls += 1


class _FakeSchema:
    fields = []


class _FakeDf:
    schema = _FakeSchema()


def _make_table(**kwargs) -> TableConfig:
    defaults = {'name': 'test_table', 'target_name': 'test_target'}
    defaults.update(kwargs)
    return TableConfig(**defaults)


@pytest.fixture
def recorder(monkeypatch):
    log = _RecordingLogger()
    monkeypatch.setattr(api, 'logger', log)
    return log


@pytest.fixture
def sleeper(monkeypatch):
    calls = []
    monkeypatch.setattr(api.time, 'sleep', lambda seconds: calls.append(seconds))
    return calls


def _run(table, extractor, backend=None, transform_fn=None):
    backend = backend or _FakeBackend()
    api._run_table(
        extractor=extractor,
        loader=_FakeLoader(),
        table=table,
        backend=backend,
        spark=None,
        pipeline_name='test_pipeline',
        transform_fn=transform_fn,
    )
    return backend


class TestRetryPolicy:
    def test_success_on_first_attempt_does_not_retry(self, recorder, sleeper):
        extractor = _FakeExtractor()
        backend = _run(_make_table(), extractor)

        assert extractor.calls == 1
        assert recorder.by_level('warning') == []
        assert recorder.by_level('error') == []
        assert recorder.by_level('exception') == []
        assert backend.statuses() == ['extracting']
        assert sleeper == []

    def test_transient_failure_retried_then_succeeds(self, recorder, sleeper):
        extractor = _FakeExtractor([RuntimeError('connection attempt failed')])
        table = _make_table(max_retries=2, retry_delay=5)
        backend = _run(table, extractor)

        assert extractor.calls == 2
        assert recorder.statuses('warning') == ['retrying']
        assert recorder.by_level('error') == []
        assert recorder.by_level('exception') == []
        retry_log = recorder.by_level('warning')[0]
        assert retry_log['attempt'] == 1
        assert retry_log['max_retries'] == 2
        assert retry_log['retry_delay'] == 5
        assert 'connection attempt failed' in retry_log['error']
        assert sleeper == [5]
        assert 'failed' in backend.statuses()

    def test_exhausted_retries_logs_single_error_and_raises(self, recorder, sleeper):
        extractor = _FakeExtractor([RuntimeError('boom')] * 3)
        table = _make_table(max_retries=2, retry_delay=1)

        with pytest.raises(RuntimeError, match='boom'):
            _run(table, extractor)

        assert extractor.calls == 3
        assert recorder.statuses('warning') == ['retrying', 'retrying']
        assert recorder.statuses('exception') == ['failed']
        assert sleeper == [1, 1]

    def test_max_retries_zero_does_not_retry(self, recorder, sleeper):
        extractor = _FakeExtractor([RuntimeError('boom')])
        table = _make_table(max_retries=0)

        with pytest.raises(RuntimeError):
            _run(table, extractor)

        assert extractor.calls == 1
        assert recorder.statuses('warning') == []
        assert recorder.statuses('exception') == ['failed']
        assert sleeper == []

    def test_pass_on_error_swallows_after_exhausting(self, recorder, sleeper):
        extractor = _FakeExtractor([RuntimeError('boom')] * 2)
        table = _make_table(max_retries=1, pass_on_error=True)

        _run(table, extractor)

        assert extractor.calls == 2
        assert recorder.statuses('exception') == ['failed']

    def test_negative_max_retries_treated_as_zero(self, recorder, sleeper):
        extractor = _FakeExtractor([RuntimeError('boom')])
        table = _make_table(max_retries=-5)

        with pytest.raises(RuntimeError):
            _run(table, extractor)

        assert extractor.calls == 1

    def test_default_retry_policy_is_two_attempts(self, recorder, sleeper):
        extractor = _FakeExtractor([RuntimeError('boom')] * 3)

        with pytest.raises(RuntimeError):
            _run(_make_table(), extractor)

        assert extractor.calls == 3
        assert recorder.statuses('warning') == ['retrying', 'retrying']


class TestNonRetryableErrors:
    def test_config_error_not_retried(self, recorder, sleeper):
        extractor = _FakeExtractor([ConfigError('bad config')])
        table = _make_table(max_retries=3)

        with pytest.raises(ConfigError):
            _run(table, extractor)

        assert extractor.calls == 1
        assert recorder.statuses('warning') == []
        assert recorder.statuses('exception') == ['failed']
        assert sleeper == []

    def test_transform_error_not_retried(self, recorder, sleeper):
        def _bad_transform(df):
            raise ValueError('transform blew up')

        class _DfExtractor(_FakeExtractor):
            def extract(self, table, spark, last_point=None):
                self.calls += 1
                return ExtractResult(df=_FakeDf(), write_mode='overwrite', last_point_value=None)

        table = _make_table(max_retries=3)
        with pytest.raises(api.TransformError):
            _run(table, _DfExtractor(), transform_fn=_bad_transform)

        assert recorder.statuses('warning') == []
        assert recorder.statuses('exception') == ['failed']
        assert sleeper == []


class TestInProgressGuard:
    def test_skips_when_table_already_in_progress(self, recorder, sleeper):
        extractor = _FakeExtractor()
        backend = _FakeBackend(initial_status='extracting')

        _run(_make_table(), extractor, backend=backend)

        assert extractor.calls == 0
        assert recorder.statuses('info') == ['skipped']
        assert backend.status_updates == []
