# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
import json

from typing import Dict, Iterator, Union, Optional

from databuilder.extractor.es_base_extractor import ElasticsearchBaseExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


class ElasticsearchIndexExtractor(ElasticsearchBaseExtractor):
    """
    Extractor to extract index metadata from Elasticsearch
    """

    def get_scope(self) -> str:
        return 'extractor.es_indexes'

    def _render_programmatic_description(self, input: Optional[Dict]) -> str:
        if input:
            result = f"""```\n{json.dumps(input, indent=2)}\n```"""

            print(result)

            return result
        else:
            return None

    def _get_extract_iter(self) -> Iterator[Union[TableMetadata, None]]:
        indexes: Dict = self._get_indexes()

        for index_name, index_metadata in indexes.items():
            mappings = index_metadata.get('mappings', dict())

            try:
                # Elasticsearch supports single document type per index but this type can have any arbitrary name.
                doc_type = list(mappings.keys())[0]
            except IndexError:
                continue

            properties = mappings.get(doc_type, dict()).get('properties', dict()) or dict()

            columns = []

            for column_name, column_metadata in properties.items():
                column_metadata = ColumnMetadata(name=column_name,
                                                 description='',
                                                 col_type=column_metadata.get('type', ''),
                                                 sort_order=0)
                columns.append(column_metadata)

            table_metadata = TableMetadata(database=self.database,
                                           cluster=self.cluster,
                                           schema=self.schema,
                                           name=index_name,
                                           description=None,
                                           columns=columns,
                                           is_view=False,
                                           tags=None,
                                           description_source=None)

            yield table_metadata

            if self._extract_technical_details:
                _settings = index_metadata.get('settings', dict())
                _aliases = index_metadata.get('aliases', dict())

                settings = self._render_programmatic_description(_settings)
                aliases = self._render_programmatic_description(_aliases)

                if aliases:
                    yield TableMetadata(database=self.database,
                                        cluster=self.cluster,
                                        schema=self.schema,
                                        name=index_name,
                                        description=aliases,
                                        columns=columns,
                                        is_view=False,
                                        tags=None,
                                        description_source='aliases')
                if settings:
                    yield TableMetadata(database=self.database,
                                        cluster=self.cluster,
                                        schema=self.schema,
                                        name=index_name,
                                        description=settings,
                                        columns=columns,
                                        is_view=False,
                                        tags=None,
                                        description_source='settings')
