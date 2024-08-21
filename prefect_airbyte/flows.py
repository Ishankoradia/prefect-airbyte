"""Flows for interacting with Airbyte."""

from prefect import flow, task

from prefect_airbyte.connections import (
    AirbyteConnection,
    AirbyteSyncResult,
    AirbyteSync,
    ResetStream,
)
from prefect_airbyte.utils import sort_dict


@flow
async def run_connection_sync(
    airbyte_connection: AirbyteConnection,
) -> AirbyteSyncResult:
    """A flow that triggers a sync of an Airbyte connection and waits for it to complete.

    Args:
        airbyte_connection: `AirbyteConnection` representing the Airbyte connection to
            trigger and wait for completion of.

    Returns:
        `AirbyteSyncResult`: Model containing metadata for the `AirbyteSync`.

    Example:
        Define a flow that runs an Airbyte connection sync:
        ```python
        from prefect import flow
        from prefect_airbyte.server import AirbyteServer
        from prefect_airbyte.connections import AirbyteConnection
        from prefect_airbyte.flows import run_connection_sync

        airbyte_server = AirbyteServer(
            server_host="localhost",
            server_port=8000
        )

        connection = AirbyteConnection(
            airbyte_server=airbyte_server,
            connection_id="<YOUR-AIRBYTE-CONNECTION-UUID>"
        )

        @flow
        def airbyte_sync_flow():
            # do some things

            airbyte_sync_result = run_connection_sync(
                airbyte_connection=connection
            )
            print(airbyte_sync_result.records_synced)

            # do some other things, like trigger DBT based on number of new raw records
        ```
    """

    # TODO: refactor block method calls to avoid using <sync_compatible_method>.aio
    # we currently need to do this because of the deadlock caused by calling
    # a sync task within an async flow
    # see [this issue](https://github.com/PrefectHQ/prefect/issues/7551)

    airbyte_sync = await task(airbyte_connection.trigger.aio)(airbyte_connection)

    await task(airbyte_sync.wait_for_completion.aio)(airbyte_sync)

    return await task(airbyte_sync.fetch_result.aio)(airbyte_sync)


@flow
async def reset_connection(
    airbyte_connection: AirbyteConnection,
) -> AirbyteSyncResult:
    """A flow that triggers a reset of an Airbyte connection and waits for it to complete.

    Args:
        airbyte_connection: `AirbyteConnection` representing the Airbyte connection to
            trigger and wait for completion of.

    Returns:
        `AirbyteSyncResult`: Model containing metadata for the `AirbyteSync`.

    Example:
        Define a flow that runs an Airbyte connection sync:
        ```python
        from prefect import flow
        from prefect_airbyte.server import AirbyteServer
        from prefect_airbyte.connections import AirbyteConnection
        from prefect_airbyte.flows import run_connection_sync

        airbyte_server = AirbyteServer(
            server_host="localhost",
            server_port=8000
        )

        connection = AirbyteConnection(
            airbyte_server=airbyte_server,
            connection_id="<YOUR-AIRBYTE-CONNECTION-UUID>"
        )

        @flow
        def airbyte_sync_flow():
            # do some things

            airbyte_sync_result = run_connection_sync(
                airbyte_connection=connection
            )
            print(airbyte_sync_result.records_synced)

            # do some other things, like trigger DBT based on number of new raw records
        ```
    """

    # TODO: refactor block method calls to avoid using <sync_compatible_method>.aio
    # we currently need to do this because of the deadlock caused by calling
    # a sync task within an async flow
    # see [this issue](https://github.com/PrefectHQ/prefect/issues/7551)

    reset_job: AirbyteSync = await task(airbyte_connection.reset.aio)(
        airbyte_connection
    )

    await task(reset_job.wait_for_completion.aio)(reset_job)

    return await task(reset_job.fetch_result.aio)(reset_job)


@flow
async def reset_connection_streams(
    airbyte_connection: AirbyteConnection, streams: list[ResetStream]
) -> None:
    """A flow that triggers a reset for the defined streams of an Airbyte connection and waits for it to complete.

    Args:
        airbyte_connection: `AirbyteConnection` representing the Airbyte connection.
        streams: list[ResetStream] representing the streams that need to be reset


    Returns:
        None

    Example:
        Define a flow that runs an Airbyte connection sync:
        ```python
        from prefect import flow
        from prefect_airbyte.server import AirbyteServer
        from prefect_airbyte.connections import AirbyteConnection
        from prefect_airbyte.flows import reset_connection_streams

        airbyte_server = AirbyteServer(
            server_host="localhost",
            server_port=8000
        )

        connection = AirbyteConnection(
            airbyte_server=airbyte_server,
            connection_id="<YOUR-AIRBYTE-CONNECTION-UUID>"
        )

        @flow
        def airbyte_reset_connection_streams_flow():
            # do some things

            airbyte_sync_result = reset_connection_streams(
                airbyte_connection=connection, streams=streams
            )

        ```
    """

    reset_job: AirbyteSync = await task(airbyte_connection.reset_streams.aio)(
        airbyte_connection, streams
    )

    await task(reset_job.wait_for_completion.aio)(reset_job)

    return await task(reset_job.fetch_result.aio)(reset_job)


@flow
async def refresh_schema(
    airbyte_connection: AirbyteConnection, catalog_diff: dict
) -> None:
    """
    A flow that does the following
    1. Update the connection with new catalog
    2. Reset the affected streams
    3. Run a sync on the connection

    Args:
        airbyte_connection: `AirbyteConnection` representing the Airbyte connection.
        catalog_diff: the diff of the changes to be applied at the time when the schema refresh was triggered


    Returns:
        None

    Example:
        Define a flow that runs an Airbyte connection sync:
        ```python
        from prefect import flow
        from prefect_airbyte.server import AirbyteServer
        from prefect_airbyte.connections import AirbyteConnection
        from prefect_airbyte.flows import refresh_schema

        airbyte_server = AirbyteServer(
            server_host="localhost",
            server_port=8000
        )

        connection = AirbyteConnection(
            airbyte_server=airbyte_server,
            connection_id="<YOUR-AIRBYTE-CONNECTION-UUID>"
        )

        catalog_diff = {
            "transforms": [
                {
                    "transformType": "update_stream",
                    "streamDescriptor": {
                        "name": "some-stream-name"
                    },
                    "updateStream": [
                        {
                            "transformType": "add_field",
                            "fieldName": [
                                "some-new-field"
                            ],
                            "breaking": false,
                            "addField": {
                                "schema": {
                                    "type": "string"
                                }
                            }
                        }
                    ]
                }
            ]
        }

        @flow
        def airbyte_refresh_schema_for_connection():
            # do some things

            refresh_schema(
                airbyte_connection=connection, catalog_diff=catalog_diff
            )

        ```
    """
    affected_streams = []
    # fetch the current catalog diff
    async with airbyte_connection.airbyte_server.get_client(
        logger=airbyte_connection.logger,
        timeout=airbyte_connection.timeout,
    ) as airbyte_client:
        conn = airbyte_client.get_webbackend_connection(
            airbyte_connection.connection_id, refresh_catalog=True
        )

        # compare the diff fetched above with the diff passed in the function
        # if they are different, abort
        if sort_dict(conn["catalogDiff"]) != sort_dict(catalog_diff):
            raise ValueError(
                "The catalog diff provided does not match the current catalog diff. Please run with the latest catalog diff"
            )

        affected_streams = [
            transform["streamDescriptor"]["name"]
            for transform in conn["catalogDiff"].get("transforms", [])
            if transform.get("streamDescriptor")
        ]

        # update the connection with the new catalog
        airbyte_client.update_webbackend_connection(conn, skip_reset=True)

    if len(affected_streams) > 0:

        # reset the affected streams
        reset_connection_streams(
            airbyte_connection=airbyte_connection,
            streams=[
                ResetStream(stream_name=stream_name)
                for stream_name in list(set(affected_streams))
            ],
        )

        # run a sync on the connection
        run_connection_sync(airbyte_connection=airbyte_connection)
