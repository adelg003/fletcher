"""Locust load testing script for Fletcher API.

This module provides load testing capabilities for the Fletcher data orchestration
platform using Locust. It simulates realistic user workflows including plan
creation, authentication, and data product state updates.
"""

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from random import choice
from time import sleep
from typing import Any
from uuid import UUID, uuid4

from locust import HttpUser, between, task
from pydantic import BaseModel

HOST: str = "http://0.0.0.0:3000"
SERVICE: str = "local"
KEY: str = "abc123"


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


class FletcherUser(HttpUser):
    """Locust user class for load testing Fletcher API."""

    host: str | None = HOST
    wait_time: Callable = between(1, 3)

    def on_start(self) -> None:
        """Initialize the user by authenticating and creating a test plan."""
        auth_payload: AuthLogin = AuthLogin(service=SERVICE, key=KEY)

        # Authenticate against the API
        auth_response: dict[str, Any] = self.client.post(
            url="/api/authenticate",
            json=auth_payload.model_dump(mode="json"),
        ).json()

        self.auth: Auth = Auth.model_validate(auth_response)

        # What is out plan?
        plan_payload: PlanPost = generate_plan_payload()

        # Register our plan
        response: dict[str, Any] = self.client.post(
            url="/api/plan",
            json=plan_payload.model_dump(mode="json"),
            headers={"Authorization": f"Bearer {self.auth.access_token}"},
        ).json()

        self.plan: Plan = Plan.model_validate(response)

    @task
    def check_ui(self) -> None:
        """Test the Fletcher web UI by accessing various pages."""
        # Pull up the home page
        self.client.get("/")
        sleep(2)

        # Search the home page
        self.client.get(
            "/component/plan_search",
            params={"page": 0, "search_by": self.plan.dataset.id},
        )
        sleep(2)

        # Load the page for our dataset
        self.client.get(f"/plan/{self.plan.dataset.id}")

    @task(4)
    def trigger_next_step(self) -> None:
        """Simulate data product state transitions in the Fletcher pipeline."""
        # What at the next eager step we can trigger?
        next_eager_dp: list[UUID] = [
            dp.id
            for dp in self.plan.data_products
            if dp.eager and dp.state == State.QUEUED
        ]

        # What at the next non-eager step we can trigger?
        next_noneager_dp: list[UUID] = [
            dp.id
            for dp in self.plan.data_products
            if not dp.eager
            and dp.state == State.WAITING
            # All upsteam steps must be "success"
            and all(
                next(
                    dp2.state == State.SUCCESS
                    for dp2 in self.plan.data_products
                    if dp2.id == dep.parent_id
                )
                for dep in self.plan.dependencies
                if dep.child_id == dp.id
            )
        ]

        # What steps are currently running?
        next_running_dp: list[UUID] = [
            dp.id for dp in self.plan.data_products if dp.state == State.RUNNING
        ]

        # Pick one
        dp_id: UUID = choice(next_eager_dp + next_noneager_dp + next_running_dp)  # noqa: S311

        # Selected data products state
        state: State = next(
            dp.state for dp in self.plan.data_products if dp.id == dp_id
        )

        # What to do with our Data product
        match state:
            # Lets set to running
            case State.WAITING | State.QUEUED:
                self.update_data_product(dp_id=dp_id, state=State.RUNNING)

                # Simulate the running of content
                sleep(10)

            # Set state to done
            case State.RUNNING:
                self.update_data_product(dp_id=dp_id, state=State.SUCCESS)

                # Are we done?
                all_done: bool = all(
                    dp.state == State.SUCCESS for dp in self.plan.data_products
                )

                if all_done:
                    self.stop()

            # Well, we should not end up here
            case _:
                error_msg = f"Somehow got Data Product ID: {dp_id}, State: {state}"
                raise RuntimeError(error_msg)

    def update_data_product(self, dp_id: UUID, state: State) -> None:
        """Update the state of a data product via Fletcher API.

        Args:
            dp_id: UUID of the data product to update
            state: New state to set for the data product

        """
        update_payload: list[dict[str, Any]] = [
            DataProductPut(id=dp_id, state=state).model_dump(mode="json"),
        ]

        response: dict[str, Any] = self.client.put(
            url=f"/api/data_product/{self.plan.dataset.id}/update",
            json=update_payload,
            headers={"Authorization": f"Bearer {self.auth.access_token}"},
        ).json()

        self.plan = Plan.model_validate(response)


def generate_plan_payload() -> PlanPost:
    """Generate a complex test plan with multiple data products and dependencies.

    Returns:
        PlanPost: A plan configuration for testing Fletcher's orchestration capabilities

    """
    dataset_id: UUID = uuid4()
    bkpf_id: UUID = uuid4()
    bseg_id: UUID = uuid4()
    t001_id: UUID = uuid4()
    glt0_id: UUID = uuid4()
    edm_gl_id: UUID = uuid4()
    edm_tb_id: UUID = uuid4()
    alchemy_id: UUID = uuid4()
    fsli_id: UUID = uuid4()
    mida_id: UUID = uuid4()
    journals_1000_id: UUID = uuid4()
    journals_2000_id: UUID = uuid4()

    return PlanPost(
        dataset=DatasetPost(id=dataset_id, paused=False),
        data_products=[
            DataProductPost(
                id=bkpf_id,
                compute=Compute.CAMS,
                name="BKPF",
                version="1.0.0",
                eager=False,
            ),
            DataProductPost(
                id=bseg_id,
                compute=Compute.CAMS,
                name="BSEG",
                version="1.0.0",
                eager=False,
            ),
            DataProductPost(
                id=t001_id,
                compute=Compute.CAMS,
                name="T001",
                version="1.0.0",
                eager=False,
            ),
            DataProductPost(
                id=glt0_id,
                compute=Compute.CAMS,
                name="GLT0",
                version="1.0.0",
                eager=False,
            ),
            DataProductPost(
                id=edm_gl_id,
                compute=Compute.CAMS,
                name="EDM_GL",
                version="3.0.0",
                eager=True,
            ),
            DataProductPost(
                id=edm_tb_id,
                compute=Compute.CAMS,
                name="EDM_TB",
                version="3.0.0",
                eager=True,
            ),
            DataProductPost(
                id=alchemy_id,
                compute=Compute.CAMS,
                name="Alchemy",
                version="1.0.0",
                eager=True,
            ),
            DataProductPost(
                id=fsli_id,
                compute=Compute.CAMS,
                name="FSLI Mapping",
                version="1.0.0",
                eager=False,
            ),
            DataProductPost(
                id=mida_id,
                compute=Compute.CAMS,
                name="MIDA",
                version="1.0.0",
                eager=True,
            ),
            DataProductPost(
                id=journals_1000_id,
                compute=Compute.CAMS,
                name="Journals",
                version="1.0.0",
                eager=True,
                passthrough={"comp_code": 1000},
            ),
            DataProductPost(
                id=journals_2000_id,
                compute=Compute.CAMS,
                name="Journals",
                version="1.0.0",
                eager=False,
                passthrough={"comp_code": 2000},
            ),
        ],
        dependencies=[
            DependencyPost(
                parent_id=bkpf_id,
                child_id=edm_gl_id,
                extra={"desc": "BKPF -> EDM_GL"},
            ),
            DependencyPost(
                parent_id=bseg_id,
                child_id=edm_gl_id,
                extra={"desc": "BSEG -> EDM_GL"},
            ),
            DependencyPost(
                parent_id=t001_id,
                child_id=edm_gl_id,
                extra={"desc": "T001 -> EDM_GL"},
            ),
            DependencyPost(
                parent_id=bseg_id,
                child_id=edm_tb_id,
                extra={"desc": "BSEG -> EDM_TB"},
            ),
            DependencyPost(
                parent_id=t001_id,
                child_id=edm_tb_id,
                extra={"desc": "T001 -> EDM_TB"},
            ),
            DependencyPost(
                parent_id=glt0_id,
                child_id=edm_tb_id,
                extra={"desc": "GLT0 -> EDM_TB"},
            ),
            DependencyPost(
                parent_id=edm_gl_id,
                child_id=alchemy_id,
                extra={"desc": "EDM_GL -> Alchemy"},
            ),
            DependencyPost(
                parent_id=edm_tb_id,
                child_id=alchemy_id,
                extra={"desc": "EDM_TB -> Alchemy"},
            ),
            DependencyPost(
                parent_id=edm_gl_id,
                child_id=journals_1000_id,
                extra={"desc": "EDM_GL -> Journals:1000"},
            ),
            DependencyPost(
                parent_id=edm_tb_id,
                child_id=journals_1000_id,
                extra={"desc": "EDM_TB -> Journals:1000"},
            ),
            DependencyPost(
                parent_id=fsli_id,
                child_id=journals_1000_id,
                extra={"desc": "FSLI Mapping -> Journals:1000"},
            ),
            DependencyPost(
                parent_id=mida_id,
                child_id=journals_1000_id,
                extra={"desc": "MIDA -> Journals:1000"},
            ),
            DependencyPost(
                parent_id=edm_gl_id,
                child_id=journals_2000_id,
                extra={"desc": "EDM_GL -> Journals:2000"},
            ),
            DependencyPost(
                parent_id=edm_tb_id,
                child_id=journals_2000_id,
                extra={"desc": "EDM_TB -> Journals:2000"},
            ),
            DependencyPost(
                parent_id=fsli_id,
                child_id=journals_2000_id,
                extra={"desc": "FSLI Mapping -> Journals:2000"},
            ),
            DependencyPost(
                parent_id=mida_id,
                child_id=journals_2000_id,
                extra={"desc": "MIDA -> Journals:2000"},
            ),
        ],
    )
