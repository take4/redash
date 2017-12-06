import json
import logging
import datetime
from redash import settings
from redash.query_runner import *
from redash.utils import JSONEncoder
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table import EntityProperty

logger = logging.getLogger(__name__)

TYPES_MAP = {
    str: TYPE_STRING,
    unicode: TYPE_STRING,
    int: TYPE_INTEGER,
    long: TYPE_INTEGER,
    float: TYPE_FLOAT,
    bool: TYPE_BOOLEAN,
    datetime.datetime: TYPE_DATETIME,
}

def json_serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(repr(obj) + " is not JSON serializable")

class TableStorage(BaseQueryRunner):
    noop_query = ''

    def run_query(self, query, user):
        try:
            table_service = TableService(
                account_name=self.configuration["account_name"],
                account_key=self.configuration["account_key"])
            entities = table_service.query_entities(
                table_name=self.configuration["table_name"],
                filter=query)

            columns = self.fetch_columns(self._column_types(entities))
            rows = [dict(zip((c['name'] for c in columns), self._entity_values(entity))) for entity in entities]
            data = {'columns': columns, 'rows': rows}
            error = None
            json_data = json.dumps(data, default=json_serialize_datetime)
        except Exception as ex:
            json_data = None
            logger.error(ex.message)
            error = ex.message

        return json_data, error

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'account_name': {
                    'type': 'string'
                },
                'account_key': {
                    'type': 'string'
                },
                'table_name': {
                    'type': 'string'
                }
            },
            'required': ['account_name', 'account_key'],
            'secret': ['account_key']
        }
    @classmethod
    def enabled(cls):
        return True

    @classmethod
    def annotate_query(cls):
        return False

    def __init__(self, configuration):
        super(TableStorage, self).__init__(configuration)

    def _column_types(self, entities):
        column_types = []
        for k, v in entities.items[0].items():
            if k == 'etag':
                continue
            type_ = None
            if isinstance(v, EntityProperty):
                type_ = type(v.value)
            else:
                type_ = type(v)
            column_types.append((str(k), TYPES_MAP.get(type_, None)))
        return column_types

    def _entity_values(self, entity):
        values = []
        for k, v in entity.items():
            if k == 'etag':
                continue
            if isinstance(v, EntityProperty):
                values.append(v.value)
            else:
                values.append(v)
        return values

register(TableStorage)
