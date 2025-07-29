from datetime import datetime
from enum import Enum
from random import random
from uuid import UUID, uuid4
from collections.abc import Callable
from time import sleep
from typing import Any, Dict, List, NotRequired, Optional, TypeAlias, TypedDict

from pydantic import BaseModel
from locust import HttpUser, between, task
from random import choice


class Role(str, Enum):
    DISABLE = "disable"
    PUBLISH = "publish"
    PAUSE = "pause"
    UPDATE = "update"


class Auth(BaseModel):
    access_token: str
    expires: int
    issued: int
    issued_by: str
    roles: List[Role]
    service: str
    token_type: str
    ttl: int


class AuthLogin(BaseModel):
    service: str
    key: str


class Dataset(BaseModel):
    id: UUID
    paused: bool
    extra: Any = None
    modified_by: str
    modified_date: datetime


class Compute(str, Enum):
    CAMS = "cams"
    DBXAAS = "dbxaas"


class State(str, Enum):
    DISABLED = "disabled"
    FAILED = "failed"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    WAITING = "waiting"


class DataProduct(BaseModel):
    id: UUID
    compute: Compute
    name: str
    version: str
    eager: bool
    passthrough: Any = None
    state: State
    run_id: Optional[UUID] = None
    link: Optional[str] = None
    passback: Any = None
    extra: Any = None
    modified_by: str
    modified_date: datetime


class Dependency(BaseModel):
    parent_id: UUID
    child_id: UUID
    extra: Any = None
    modified_by: str
    modified_date: datetime


class Plan(BaseModel):
    dataset: Dataset
    data_products: List[DataProduct]
    dependencies: List[Dependency]


class DatasetPost(BaseModel):
    id: UUID
    paused: bool


class DataProductPost(BaseModel):
    id: UUID
    compute: Compute
    name: str
    version: str
    eager: bool
    passthrough: Any = None


class DependencyPost(BaseModel):
    parent_id: UUID
    child_id: UUID
    extra: Any = None


class PlanPost(BaseModel):
    dataset: DatasetPost
    data_products: List[DataProductPost]
    dependencies: List[DependencyPost]


class DataProductPut(BaseModel):
    id: UUID
    state: State


class FletcherUser(HttpUser):
    host: Optional[str] = "http://0.0.0.0:3000"
    wait_time: Callable = between(1, 3)

    def on_start(self) -> None:
        # Authenticate against the API
        auth_response: Dict[str, Any] = self.client.post(
            url="/api/authenticate",
            json=AuthLogin(service="local", key="abc123"),
        ).json()

        self.auth: Auth = Auth.model_validate(auth_response)

        # What is out plan?
        plan_param: PlanPost = generate_plan_param()

        # Register our plan
        response: Dict[str, Any] = self.client.post(
            url="/plan",
            json=plan_param.model_dump(),
            headers={"Authorization": f"Bearer {self.auth.access_token}"},
        ).json()

        self.plan: Plan = Plan.model_validate(response)

    @task(4)
    def trigger_next_step(self) -> None:
        # What at the next eager step we can trigger?
        next_eager_dp: List[UUID] = [
            dp.id
            for dp in self.plan.data_products
            if dp.eager and dp.state == State.QUEUED
        ]

        # What at the next non-eager step we can trigger?
        next_noneager_dp: List[UUID] = [
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
        next_running_dp: List[UUID] = [
            dp.id for dp in self.plan.data_products if dp.state == State.RUNNING
        ]

        # Pick one
        dp_id: UUID = choice(next_eager_dp + next_noneager_dp + next_running_dp)

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
                    #self.interrupt(reschdual=False)

            # Well, we should not end up here
            case _:
                raise RuntimeError(f"Somehow get DP ID: {dp_id}, State: {state}")

    # Update the state of a data product
    def update_data_product(self, dp_id: UUID, state: State) -> None:
        update: List[Dict[str, Any]] = [
            DataProductPut(id=dp_id, state=state).model_dump()
        ]

        response: Dict[str, Any] = self.client.put(
            url=f"/data_product/{dp_id}/update",
            json=update,
            headers={"Authorization": f"Bearer {self.auth.access_token}"},
        ).json()

        self.plan = Plan.model_validate(response)


# What plan are we going to test?
def generate_plan_param() -> PlanPost:
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
