# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
import abc

from pyhocon import ConfigTree
from typing import Any, Dict, Iterator, Union

from databuilder.extractor.generic_extractor import GenericExtractor


class ElasticsearchBaseExtractor(GenericExtractor):
    """
    Extractor to extract index metadata from Elasticsearch
    """

    ELASTICSEARCH_CLIENT_CONFIG_KEY = 'client'
    ELASTICSEARCH_EXTRACT_TECHNICAL_DETAILS = 'extract_technical_details'

    PRODUCT = 'product'
    CLUSTER = 'cluster'
    SCHEMA = 'schema'

    def __init__(self) -> None:
        super(ElasticsearchBaseExtractor, self).__init__()

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self._extract_iter = self._get_extract_iter()

        self.es = self.conf.get(ElasticsearchBaseExtractor.ELASTICSEARCH_CLIENT_CONFIG_KEY)

    def _get_indexes(self) -> Dict:
        result = dict()

        try:
            _indexes = self.es.indices.get('*')

            for k, v in _indexes.items():
                if not k.startswith('.'):
                    result[k] = v
        except Exception:
            pass

        return result

    def extract(self) -> Any:
        try:
            result = next(self._extract_iter)

            return result
        except StopIteration:
            return None

    @property
    def database(self) -> str:
        return 'elasticsearch'

    @property
    def cluster(self) -> str:
        return self.conf.get(ElasticsearchBaseExtractor.CLUSTER)

    @property
    def schema(self) -> str:
        return self.conf.get(ElasticsearchBaseExtractor.SCHEMA)

    @property
    def _extract_technical_details(self) -> bool:
        try:
            return self.conf.get(ElasticsearchBaseExtractor.ELASTICSEARCH_EXTRACT_TECHNICAL_DETAILS)
        except Exception:
            return False

    @abc.abstractmethod
    def _get_extract_iter(self) -> Iterator[Union[Any, None]]:
        pass
