# -*- coding: utf-8 -*-
"""OpenLineage event emission (START / COMPLETE / FAIL) per pipeline stage."""

from datetime import datetime, timezone
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.uuid import generate_new_uuid

PRODUCER = "electricity-capstone-pipeline"
NAMESPACE = "electricity_pipeline"

# Point this at your OpenLineage backend (e.g. a local Marquez instance).
client = OpenLineageClient(url="http://localhost:5000")


def _now():
    return datetime.now(timezone.utc).isoformat()


def emit_event(job_name: str, state: RunState, run_id: str = None):
    run_id = run_id or str(generate_new_uuid())
    event = RunEvent(
        eventType=state,
        eventTime=_now(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=job_name),
        producer=PRODUCER,
    )
    try:
        client.emit(event)
        print(f"🔗 Lineage: {job_name} -> {state.name} ({run_id})")
    except Exception as e:
        # Lineage emission failing should never take down the actual pipeline.
        print(f"⚠️ Lineage emit failed (non-blocking): {e}")
    return run_id


def track_stage(job_name):
    """Decorator: emits START before, COMPLETE after success, FAIL on exception."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            run_id = emit_event(job_name, RunState.START)
            try:
                result = func(*args, **kwargs)
                emit_event(job_name, RunState.COMPLETE, run_id)
                return result
            except Exception:
                emit_event(job_name, RunState.FAIL, run_id)
                raise
        return wrapper
    return decorator
