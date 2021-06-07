# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
from typing import (
    Iterator, List, Optional, Union,
)

from amundsen_common.utils.atlas import (
    AtlasCommonParams, AtlasCommonTypes, AtlasTableTypes,
)
from amundsen_rds.models import RDSModel
from amundsen_rds.models.table import TableOwner as RDSTableOwner
from amundsen_rds.models.user import User as RDSUser

from databuilder.models.atlas_entity import AtlasEntity
from databuilder.models.atlas_relationship import AtlasRelationship
from databuilder.models.atlas_serializable import AtlasSerializable
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship
from databuilder.models.graph_serializable import GraphSerializable
from databuilder.models.owner_constants import OWNER_OF_OBJECT_RELATION_TYPE, OWNER_RELATION_TYPE
from databuilder.models.table_serializable import TableSerializable
from databuilder.models.user import User
from databuilder.serializers.atlas_serializer import get_entity_attrs
from databuilder.utils.atlas import AtlasRelationshipTypes, AtlasSerializedEntityOperation


class TableOwner(GraphSerializable, TableSerializable, AtlasSerializable):
    """
    Hive table owner model.
    """
    OWNER_TABLE_RELATION_TYPE = OWNER_OF_OBJECT_RELATION_TYPE
    TABLE_OWNER_RELATION_TYPE = OWNER_RELATION_TYPE

    def __init__(self,
                 db_name: str,
                 schema: str,
                 table_name: str,
                 owners: Union[List, str],
                 cluster: str = 'gold',
                 ) -> None:
        self.db = db_name
        self.schema = schema
        self.table = table_name
        if isinstance(owners, str):
            owners = owners.split(',')
        self.owners = [owner.strip() for owner in owners]

        self.cluster = cluster
        self._node_iter = self._create_node_iterator()
        self._relation_iter = self._create_relation_iterator()
        self._record_iter = self._create_record_iterator()
        self._atlas_entity_iterator = self._create_next_atlas_entity()
        self._atlas_relation_iterator = self._create_atlas_relation_iterator()

    def create_next_node(self) -> Optional[GraphNode]:
        # return the string representation of the data
        try:
            return next(self._node_iter)
        except StopIteration:
            return None

    def create_next_relation(self) -> Optional[GraphRelationship]:
        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def create_next_record(self) -> Union[RDSModel, None]:
        try:
            return next(self._record_iter)
        except StopIteration:
            return None

    def get_owner_model_key(self, owner: str) -> str:
        return User.USER_NODE_KEY_FORMAT.format(email=owner)

    def get_metadata_model_key(self) -> str:
        return f'{self.db}://{self.cluster}.{self.schema}/{self.table}'

    def _create_node_iterator(self) -> Iterator[GraphNode]:
        """
        Create table owner nodes
        :return:
        """
        for owner in self.owners:
            if owner:
                node = GraphNode(
                    key=self.get_owner_model_key(owner),
                    label=User.USER_NODE_LABEL,
                    attributes={
                        User.USER_NODE_EMAIL: owner
                    }
                )
                yield node

    def _create_relation_iterator(self) -> Iterator[GraphRelationship]:
        """
        Create relation map between owner record with original hive table
        :return:
        """
        for owner in self.owners:
            if owner:
                relationship = GraphRelationship(
                    start_key=self.get_owner_model_key(owner),
                    start_label=User.USER_NODE_LABEL,
                    end_key=self.get_metadata_model_key(),
                    end_label='Table',
                    type=TableOwner.OWNER_TABLE_RELATION_TYPE,
                    reverse_type=TableOwner.TABLE_OWNER_RELATION_TYPE,
                    attributes={}
                )
                yield relationship

    def _create_record_iterator(self) -> Iterator[RDSModel]:
        for owner in self.owners:
            if owner:
                user_record = RDSUser(
                    rk=self.get_owner_model_key(owner),
                    email=owner
                )
                yield user_record

                table_owner_record = RDSTableOwner(
                    table_rk=self.get_metadata_model_key(),
                    user_rk=self.get_owner_model_key(owner)
                )
                yield table_owner_record

    def _create_atlas_owner_entity(self, owner: str) -> AtlasEntity:
        attrs_mapping = [
            (AtlasCommonParams.qualified_name, owner),
            ('email', owner)
        ]

        entity_attrs = get_entity_attrs(attrs_mapping)

        entity = AtlasEntity(
            typeName=AtlasCommonTypes.user,
            operation=AtlasSerializedEntityOperation.CREATE,
            attributes=entity_attrs,
            relationships=None
        )

        return entity

    def _create_atlas_owner_relation(self, owner: str) -> AtlasRelationship:
        table_relationship = AtlasRelationship(
            relationshipType=AtlasRelationshipTypes.resource_owner,
            entityType1=AtlasTableTypes.table,
            entityQualifiedName1=self.get_metadata_model_key(),
            entityType2=AtlasCommonTypes.user,
            entityQualifiedName2=self.get_owner_model_key(owner),
            attributes={}
        )

        return table_relationship

    def _create_next_atlas_entity(self) -> Iterator[AtlasEntity]:
        for owner in self.owners:
            if owner:
                yield self._create_atlas_owner_entity(owner)

    def create_next_atlas_entity(self) -> Union[AtlasEntity, None]:
        try:
            return next(self._atlas_entity_iterator)
        except StopIteration:
            return None

    def create_next_atlas_relation(self) -> Union[AtlasRelationship, None]:
        try:
            return next(self._atlas_relation_iterator)
        except StopIteration:
            return None

    def _create_atlas_relation_iterator(self) -> Iterator[AtlasRelationship]:
        for owner in self.owners:
            if owner:
                yield self._create_atlas_owner_relation(owner)

    def __repr__(self) -> str:
        return f'TableOwner({self.db!r}, {self.cluster!r}, {self.schema!r}, {self.table!r}, {self.owners!r})'
