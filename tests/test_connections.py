import pytest
from prefect.logging import disable_run_logger

from prefect_airbyte import exceptions as err
from prefect_airbyte.connections import AirbyteConnection, trigger_sync


async def example_trigger_sync_flow(connection_id, airbyte_server=None, **kwargs):
    with disable_run_logger():
        return await trigger_sync.fn(
            airbyte_server=airbyte_server, connection_id=connection_id, **kwargs
        )


async def test_successful_trigger_sync(
    mock_successful_connection_sync_calls, airbyte_server, connection_id
):
    trigger_sync_result = await example_trigger_sync_flow(
        airbyte_server=airbyte_server, connection_id=connection_id
    )

    assert type(trigger_sync_result) is dict

    assert trigger_sync_result == {
        "connection_id": "e1b2078f-882a-4f50-9942-cfe34b2d825b",
        "status": "active",
        "job_status": "succeeded",
        "job_created_at": 1650644844,
        "job_updated_at": 1650644844,
    }


async def test_cancelled_trigger_manual_sync(
    mock_cancelled_connection_sync_calls, airbyte_server, connection_id
):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await example_trigger_sync_flow(
            airbyte_server=airbyte_server, connection_id=connection_id
        )


async def test_connection_sync_inactive(
    mock_inactive_sync_calls, airbyte_server, connection_id
):
    with pytest.raises(err.AirbyteConnectionInactiveException):
        await example_trigger_sync_flow(
            airbyte_server=airbyte_server, connection_id=connection_id
        )


async def test_failed_trigger_sync(
    mock_failed_connection_sync_calls, airbyte_server, connection_id
):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await example_trigger_sync_flow(
            airbyte_server=airbyte_server, connection_id=connection_id
        )


async def test_bad_connection_id(
    mock_bad_connection_id_calls, airbyte_server, connection_id
):
    with pytest.raises(err.ConnectionNotFoundException):
        await example_trigger_sync_flow(
            airbyte_server=airbyte_server, connection_id=connection_id
        )


async def test_failed_health_check(
    mock_failed_health_check_calls, airbyte_server, connection_id
):
    with pytest.raises(err.AirbyteServerNotHealthyException):
        await example_trigger_sync_flow(
            airbyte_server=airbyte_server, connection_id=connection_id
        )


async def test_get_job_status_not_found(
    mock_invalid_job_status_calls, airbyte_server, connection_id
):
    with pytest.raises(err.JobNotFoundException):
        await example_trigger_sync_flow(
            airbyte_server=airbyte_server, connection_id=connection_id
        )


async def test_trigger_sync_with_kwargs(
    mock_successful_connection_sync_calls, connection_id
):
    trigger_sync_result = await example_trigger_sync_flow(
        airbyte_server_host="localhost",
        airbyte_server_port=8000,
        connection_id=connection_id,
    )

    assert type(trigger_sync_result) is dict

    assert trigger_sync_result == {
        "connection_id": "e1b2078f-882a-4f50-9942-cfe34b2d825b",
        "status": "active",
        "job_status": "succeeded",
        "job_created_at": 1650644844,
        "job_updated_at": 1650644844,
    }


async def test_airbyte_connection_instantiation(airbyte_server, connection_id):
    connection = AirbyteConnection(
        airbyte_server=airbyte_server,
        connection_id=connection_id,
    )

    assert isinstance(connection, AirbyteConnection)
    assert connection.airbyte_server == airbyte_server
    assert str(connection.connection_id) == connection_id


async def test_successful_cancel_job(
    mock_successful_job_cancel_calls, airbyte_connection
):
    from prefect_airbyte.connections import AirbyteSync
    from prefect.logging import disable_run_logger

    job_id = 45

    with disable_run_logger():
        cancel_result = await airbyte_connection.cancel_job.fn(job_id)

    assert isinstance(cancel_result, AirbyteSync)
    assert cancel_result.job_id == job_id
    assert cancel_result.airbyte_connection == airbyte_connection


async def test_cancel_job_not_found(
    mock_job_cancel_not_found_calls, airbyte_connection
):
    from prefect.logging import disable_run_logger

    job_id = 999

    with disable_run_logger():
        with pytest.raises(err.JobNotFoundException):
            await airbyte_connection.cancel_job.fn(job_id)


async def test_cancel_job_not_running(
    mock_job_cancel_not_running_calls, airbyte_connection
):
    from prefect.logging import disable_run_logger

    job_id = 45

    with disable_run_logger():
        with pytest.raises(err.AirbyteSyncJobFailed):
            await airbyte_connection.cancel_job.fn(job_id)
