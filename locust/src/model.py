"""Pydantic models for Fletcher API load testing.

This module contains all the data models used for interacting with the Fletcher API
during load testing. It includes models for authentication, plans, data products,
dependencies, and their various request/response formats.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class RunMode(StrEnum):
    """Execution modes for running operations."""

    ONCE = "once"
    LOOP = "loop"


class Role(StrEnum):
    """Available roles for Fletcher API authentication."""

    DISABLE = "disable"
    PUBLISH = "publish"
    PAUSE = "pause"
    UPDATE = "update"


class Auth(BaseModel):
    """Authentication response model from Fletcher API."""

    access_token: str
    expires: int
    issued: int
    issued_by: str
    roles: list[Role]
    service: str
    token_type: str
    ttl: int


class AuthLogin(BaseModel):
    """Authentication request model for Fletcher API."""

    service: str
    key: str


class Dataset(BaseModel):
    """Dataset model representing a Fletcher dataset."""

    id: UUID
    paused: bool
    extra: Any = None
    modified_by: str
    modified_date: datetime


class Compute(StrEnum):
    """Available compute platforms in Fletcher."""

    CAMS = "cams"
    DBXAAS = "dbxaas"


class State(StrEnum):
    """Possible states for data products in Fletcher."""

    DISABLED = "disabled"
    FAILED = "failed"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    WAITING = "waiting"


class DataProduct(BaseModel):
    """Data product model representing a Fletcher data product."""

    id: UUID
    compute: Compute
    name: str
    version: str
    eager: bool
    passthrough: Any = None
    state: State
    run_id: UUID | None = None
    link: UUID | None = None
    passback: Any = None
    extra: Any = None
    modified_by: str
    modified_date: datetime


class Dependency(BaseModel):
    """Dependency model representing relationships between data products."""

    parent_id: UUID
    child_id: UUID
    extra: Any = None
    modified_by: str
    modified_date: datetime


class Plan(BaseModel):
    """Complete plan model containing dataset, data products, and dependencies."""

    dataset: Dataset
    data_products: list[DataProduct]
    dependencies: list[Dependency]


class DatasetPost(BaseModel):
    """Dataset creation/update request model."""

    id: UUID
    paused: bool
    extra: Any


class DataProductPost(BaseModel):
    """Data product creation/update request model."""

    id: UUID
    compute: Compute
    name: str
    version: str
    eager: bool
    passthrough: Any = None


class DependencyPost(BaseModel):
    """Dependency creation/update request model."""

    parent_id: UUID
    child_id: UUID
    extra: Any = None


class PlanPost(BaseModel):
    """Plan creation request model."""

    dataset: DatasetPost
    data_products: list[DataProductPost]
    dependencies: list[DependencyPost]


class DataProductPut(BaseModel):
    """Data product state update request model."""

    id: UUID
    state: State
