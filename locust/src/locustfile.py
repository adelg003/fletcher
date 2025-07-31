"""Locust load testing script for Fletcher API.

This module provides load testing capabilities for the Fletcher data orchestration
platform using Locust. It simulates realistic user workflows including plan
creation, authentication, and data product state updates.
"""

from collections.abc import Callable
from random import choice
from time import sleep
from typing import Any
from uuid import UUID

from locust import HttpUser, between, task

from model import Auth, AuthLogin, DataProductPut, Plan, PlanPost, State
from setup import generate_plan_payload

HOST: str = "http://0.0.0.0:3000"
SERVICE: str = "local"
KEY: str = "abc123"


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
        sleep(5)

        # Search the home page
        self.client.get(
            "/component/plan_search",
            params={"page": 0, "search_by": self.plan.dataset.id},
        )
        sleep(5)

        # Load the page for our dataset
        self.client.get(f"/plan/{self.plan.dataset.id}")

    @task(4)
    def trigger_next_step(self) -> None:
        """Simulate data product state transitions in the Fletcher pipeline.

        Raises:
            RuntimeError: If a data product is in an unexpected state.

        """
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
                sleep(60)

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
