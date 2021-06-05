# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0


from typing import Dict, Iterator, Union

from databuilder.extractor.es_base_extractor import ElasticsearchBaseExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


class ElasticsearchIndexExtractor(ElasticsearchBaseExtractor):
    """
    Extractor to extract index metadata from Elasticsearch
    """

    def get_scope(self) -> str:
        return 'extractor.es_indexes'

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
                                           description='',
                                           columns=columns,
                                           is_view=False,
                                           tags=None,
                                           description_source=None)

            yield table_metadata
