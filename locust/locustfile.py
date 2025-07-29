from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4
from collections.abc import Callable
from time import sleep
from typing import Any, Dict, List, NotRequired, Optional, TypeAlias, TypedDict

from pydantic import BaseModel
from locust import HttpUser, between, task


# How we will authenticate agains the API
AUTH_PAYLOAD: Dict[str, str] = {"service": "local", "key": "abc123"}


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


class DatasetParam(BaseModel):
    id: UUID
    paused: bool


class DataProductParam(BaseModel):
    id: UUID
    compute: Compute
    name: str
    version: str
    eager: bool
    passthrough: Any = None


class DependencyParam(BaseModel):
    parent_id: UUID
    child_id: UUID
    extra: Any = None


class PlanParam(BaseModel):
    dataset: DatasetParam
    data_products: List[DataProductParam]
    dependencies: List[DependencyParam]


# What plan are we going to test?
def generate_plan_param() -> PlanParam:
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

    return PlanParam(
        dataset=DatasetParam(id=dataset_id, paused=False),
        data_products=[
            DataProductParam(
                id=bkpf_id,
                compute=Compute.CAMS,
                name="BKPF",
                version="1.0.0",
                eager=False,
            ),
            DataProductParam(
                id=bseg_id,
                compute=Compute.CAMS,
                name="BSEG",
                version="1.0.0",
                eager=False,
            ),
            DataProductParam(
                id=t001_id,
                compute=Compute.CAMS,
                name="T001",
                version="1.0.0",
                eager=False,
            ),
            DataProductParam(
                id=glt0_id,
                compute=Compute.CAMS,
                name="GLT0",
                version="1.0.0",
                eager=False,
            ),
            DataProductParam(
                id=edm_gl_id,
                compute=Compute.CAMS,
                name="EDM_GL",
                version="3.0.0",
                eager=True,
            ),
            DataProductParam(
                id=edm_tb_id,
                compute=Compute.CAMS,
                name="EDM_TB",
                version="3.0.0",
                eager=True,
            ),
            DataProductParam(
                id=alchemy_id,
                compute=Compute.CAMS,
                name="Alchemy",
                version="1.0.0",
                eager=True,
            ),
            DataProductParam(
                id=fsli_id,
                compute=Compute.CAMS,
                name="FSLI Mapping",
                version="1.0.0",
                eager=False,
            ),
            DataProductParam(
                id=mida_id,
                compute=Compute.CAMS,
                name="MIDA",
                version="1.0.0",
                eager=True,
            ),
            DataProductParam(
                id=journals_1000_id,
                compute=Compute.CAMS,
                name="Journals",
                version="1.0.0",
                eager=True,
                passthrough={"comp_code": 1000},
            ),
            DataProductParam(
                id=journals_2000_id,
                compute=Compute.CAMS,
                name="Journals",
                version="1.0.0",
                eager=False,
                passthrough={"comp_code": 2000},
            ),
        ],
        dependencies=[
            DependencyParam(
                parent_id=bkpf_id,
                child_id=edm_gl_id,
                extra={"desc": "BKPF -> EDM_GL"},
            ),
            DependencyParam(
                parent_id=bseg_id,
                child_id=edm_gl_id,
                extra={"desc": "BSEG -> EDM_GL"},
            ),
            DependencyParam(
                parent_id=t001_id,
                child_id=edm_gl_id,
                extra={"desc": "T001 -> EDM_GL"},
            ),
            DependencyParam(
                parent_id=bseg_id,
                child_id=edm_tb_id,
                extra={"desc": "BSEG -> EDM_TB"},
            ),
            DependencyParam(
                parent_id=t001_id,
                child_id=edm_tb_id,
                extra={"desc": "T001 -> EDM_TB"},
            ),
            DependencyParam(
                parent_id=glt0_id,
                child_id=edm_tb_id,
                extra={"desc": "GLT0 -> EDM_TB"},
            ),
            DependencyParam(
                parent_id=edm_gl_id,
                child_id=alchemy_id,
                extra={"desc": "EDM_GL -> Alchemy"},
            ),
            DependencyParam(
                parent_id=edm_tb_id,
                child_id=alchemy_id,
                extra={"desc": "EDM_TB -> Alchemy"},
            ),
            DependencyParam(
                parent_id=edm_gl_id,
                child_id=journals_1000_id,
                extra={"desc": "EDM_GL -> Journals:1000"},
            ),
            DependencyParam(
                parent_id=edm_tb_id,
                child_id=journals_1000_id,
                extra={"desc": "EDM_TB -> Journals:1000"},
            ),
            DependencyParam(
                parent_id=fsli_id,
                child_id=journals_1000_id,
                extra={"desc": "FSLI Mapping -> Journals:1000"},
            ),
            DependencyParam(
                parent_id=mida_id,
                child_id=journals_1000_id,
                extra={"desc": "MIDA -> Journals:1000"},
            ),
            DependencyParam(
                parent_id=edm_gl_id,
                child_id=journals_2000_id,
                extra={"desc": "EDM_GL -> Journals:2000"},
            ),
            DependencyParam(
                parent_id=edm_tb_id,
                child_id=journals_2000_id,
                extra={"desc": "EDM_TB -> Journals:2000"},
            ),
            DependencyParam(
                parent_id=fsli_id,
                child_id=journals_2000_id,
                extra={"desc": "FSLI Mapping -> Journals:2000"},
            ),
            DependencyParam(
                parent_id=mida_id,
                child_id=journals_2000_id,
                extra={"desc": "MIDA -> Journals:2000"},
            ),
        ],
    )


class FletcherUser(HttpUser):
    auth: Dict[str, str] = {}
    host: str | None = "http://0.0.0.0:3000"
    plan: Plan
    wait_time: Callable = between(1, 3)

    def on_start(self) -> None:
        # Authenticate against the API
        self.auth: Dict[str, str] = self.client.post(
            url="/api/authenticate",
            json=AUTH_PAYLOAD,
        ).json()

        # What is out plan?
        plan_param: PlanParam = generate_plan_param()

        # Register our plan
        plan_response: Dict = self.client.post(
            url="/plan",
            json=plan_param.model_dump(),
            headers={"Authorization": f"Bearer {self.auth['access_token']}"},
        ).json()

        self.plan = Plan.model_validate(plan_response)

    @task
    def trigger_next_step(self) -> None:
        # What at the next eager step we can trigger?
        next_eager_steps = [
            dp.id
            for dp in self.plan.data_products
            if dp.eager and dp.state == State.QUEUED
        ]

        # What at the next non-eager step we can trigger?
        next_noneager_steps = [
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
