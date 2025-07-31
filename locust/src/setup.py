"""Setup utilities for Fletcher API load testing.

This module provides functions to generate test data and configurations
for load testing the Fletcher API. It creates complex plan payloads with
realistic data product hierarchies and dependencies.
"""

from uuid import UUID, uuid4

from model import Compute, DataProductPost, DatasetPost, DependencyPost, PlanPost


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
                version="2.0.0",
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
