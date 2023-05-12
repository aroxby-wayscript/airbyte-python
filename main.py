#!/usr/bin/env python3
from datetime import datetime, timedelta

import airbyte
from airbyte.models import shared

AB_URL = 'http://localhost:8888/api'
AB_WORKSPACE_ID = '5fb60fc5-a01b-44f3-8261-f7c69b27326b'


class APIError(Exception):
    pass


def get_github_token() -> str:
    with open('github-token.txt') as token_file:
        return token_file.read().strip()


def current_datetime_str():
    now = datetime.utcnow()
    now -= timedelta(hours=4)  # Hack to get local time (container uses UTC)
    return now.isoformat()


def get_source_definition_ids(client):
    req = shared.WorkspaceIDRequestBody(
        workspace_id=AB_WORKSPACE_ID,
    )

    res = client.source_definition.list_source_definitions_for_workspace(req)

    if not res.source_definition_read_list:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )

    source_definition_ids = {
        source_definition.name: source_definition.source_definition_id
        for source_definition in res.source_definition_read_list.source_definitions
    }
    return source_definition_ids  # Github: ef69ef6e-aa7f-4af1-a01d-ef775033524e


def create_source(client, source_definition_ids):
    req = shared.SourceCreate(
        source_definition_id=source_definition_ids['GitHub'],
        connection_configuration={
            'repository': 'aroxby-wayscript/flask-sandbox',
            'start_date': datetime(2022, 3, 5).isoformat(timespec='seconds') + 'Z',
            'credentials': {
                'personal_access_token': get_github_token(),
                # option_title fixes the Web UI so it shows the correct credential type
                'option_title': 'PAT Credentials',
            },
        },
        name=f'Github ({current_datetime_str()})',
        workspace_id=AB_WORKSPACE_ID,
    )

    res = client.source.create_source(req)

    if not res.source_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )

    print(f'{res.source_read.name}:{res.source_read.source_id}')
    return res.source_read.source_id


def get_destination_definition_ids(client):
    req = shared.WorkspaceIDRequestBody(
        workspace_id=AB_WORKSPACE_ID,
    )

    res = client.destination_definition.list_destination_definitions_for_workspace(req)

    if not res.destination_definition_read_list:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )

    destination_definition_ids = {
        destination_definition.name: destination_definition.destination_definition_id
        for destination_definition in res.destination_definition_read_list.destination_definitions
    }
    return destination_definition_ids  # Postgres: 25c5221d-dce2-4163-ade9-739ef790f503


def create_destination(client, destination_definition_ids):
    req = shared.DestinationCreate(
        destination_definition_id=destination_definition_ids['Postgres'],
        connection_configuration={
            'database': 'airbyte-github',
            'host': 'host.docker.internal',
            'port': 6112,
            'schema': 'public',
            'username': 'andy',
            'password': 'swordfish',
            'ssl': False,
            'ssl_mode': {  # Required for the Web UI
                'mode': 'disable',
            },
            'tunnel_method': {
                'tunnel_method': 'NO_TUNNEL',
            }
        },
        name=f'Postgres ({current_datetime_str()})',
        workspace_id=AB_WORKSPACE_ID,
    )

    res = client.destination.create_destination(req)

    if not res.destination_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )

    print(f'{res.destination_read.name}:{res.destination_read.destination_id}')
    return res.destination_read.destination_id


def create_connection(client, source_id, destination_id):
    req = shared.ConnectionCreate(
        source_id=source_id,
        destination_id=destination_id,
        status=shared.ConnectionStatusEnum.ACTIVE,
    )

    res = client.connection.create_connection(req)

    if not res.connection_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )

    print(f'{res.connection_read.name}:{res.connection_read.connection_id}')
    return res.connection_read.connection_id


def get_connection_sync_catalog(client, connection_id):
    req = shared.WebBackendConnectionRequestBody(
        connection_id=connection_id,
        with_refreshed_catalog=True,
    )

    # TODO: Is there anyway to get the sync catalog using the standard API?
    res = client.web_backend.web_backend_get_connection(req)

    if not res.web_backend_connection_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )

    return res.web_backend_connection_read.sync_catalog


def select_all_streams(sync_catalog):
    for stream in sync_catalog.streams:
        stream.config.selected = True


def set_connection_sync_catalog(client, connection_id, sync_catalog):
    req = shared.ConnectionUpdate(
        connection_id=connection_id,
        sync_catalog=sync_catalog,
    )

    res = client.connection.update_connection(req)

    if not res.connection_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )


def create_normalization_operation(client):
    req = shared.OperationCreate(
        workspace_id=AB_WORKSPACE_ID,
        name='Normalization',
        operator_configuration=shared.OperatorConfiguration(
            operator_type=shared.OperatorTypeEnum.NORMALIZATION,
            normalization=shared.OperatorNormalization(
                option=shared.OperatorNormalizationOptionEnum.BASIC,
            ),
        ),
    )

    res = client.operation.create_operation(req)

    if not res.operation_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )

    return res.operation_read.operation_id


def add_connection_operation(client, connection_id, operation_id):
    req = shared.ConnectionUpdate(
        connection_id=connection_id,
        operation_ids=[operation_id],
    )

    res = client.connection.update_connection(req)

    if not res.connection_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )


def set_connection_schedule(client, connection_id):
    req = shared.ConnectionUpdate(
        connection_id=connection_id,
        schedule_type=shared.ConnectionScheduleTypeEnum.BASIC,
        schedule_data=shared.ConnectionScheduleData(
            basic_schedule=shared.ConnectionScheduleDataBasicSchedule(
                # Web UI only understands hours and cron
                time_unit=shared.ConnectionScheduleDataBasicScheduleTimeUnitEnum.HOURS,
                units=24,
            ),
        ),
    )

    res = client.connection.update_connection(req)

    if not res.connection_read:
        raise APIError(
            (res.raw_response.status_code, res.raw_response.content)
        )


def main():
    client = airbyte.Airbyte(server_url=AB_URL)

    source_definition_ids = get_source_definition_ids(client)
    source_id = create_source(client, source_definition_ids)

    destination_definition_ids = get_destination_definition_ids(client)
    destination_id = create_destination(client, destination_definition_ids)

    connection_id = create_connection(client, source_id, destination_id)

    sync_catalog = get_connection_sync_catalog(client, connection_id)
    select_all_streams(sync_catalog)
    set_connection_sync_catalog(client, connection_id, sync_catalog)

    operation_id = create_normalization_operation(client)
    add_connection_operation(client, connection_id, operation_id)

    # Note: This starts a sync
    set_connection_schedule(client, connection_id)


if __name__ == '__main__':
    main()
