import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient
import main


class InvoiceAgentContractTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        main.DB_PATH = self._tmp.name + "/test.sqlite3"
        main.init_db()
        self.client = TestClient(main.app)
        self.headers = {
            "Authorization": "Bearer test-user-token",
            "A2A-Version": "1.0",
            "Content-Type": "application/a2a+json",
        }
        self.original_propose = main.propose

        def fake_propose(batch, policy, packages):
            return [
                {
                    "packageId": p["packageId"],
                    "actionId": "action-" + p["packageId"] + "-123456",
                    "action": "hold_invoice",
                    "facts": {
                        "vendorName": "Example Vendor",
                        "invoiceNumber": "INV-100",
                        "amountMinor": 12345,
                        "currency": "INR",
                    },
                    "evidenceRefs": ["[REF-1]", "[REF-2]", "[REF-3]"],
                    "rationale": (
                        "hold_invoice because verification remains incomplete; "
                        "[REF-1] supports the invoice identity and [REF-2] supports the hold."
                    ),
                }
                for p in packages
            ]

        main.propose = fake_propose
        self.addCleanup(self.cleanup_test)

    def cleanup_test(self):
        main.propose = self.original_propose
        self.client.close()
        self._tmp.cleanup()

    def initial_message(self, message_id="initial-1", package_ids=("pkg-1", "pkg-2")):
        packages = [
            {
                "packageId": package_id,
                "documents": [
                    {"reference": "[REF-1]", "text": "Invoice identity"},
                    {"reference": "[REF-2]", "text": "Verification pending"},
                    {"reference": "[REF-3]", "text": "Payment state"},
                ],
            }
            for package_id in package_ids
        ]
        return {
            "messageId": message_id,
            "role": "ROLE_USER",
            "parts": [{
                "mediaType": main.INPUT_MODE,
                "data": {
                    "batchId": "batch-1",
                    "policyRevision": "policy-1",
                    "packages": packages,
                },
            }],
        }

    def create_task(self):
        response = self.client.post(
            "/a2a/message:send",
            headers=self.headers,
            json={"message": self.initial_message()},
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["task"]

    def continuation(self, task, message_id="continue-1", outcomes=None):
        proposals = task["artifacts"][0]["parts"][0]["data"]["proposals"]
        outcomes = outcomes or ["ACCEPTED"] * len(proposals)
        results = [
            {
                "packageId": proposal["packageId"],
                "actionId": proposal["actionId"],
                "action": proposal["action"],
                "outcome": outcome,
                "receiptNonce": "nonce-" + proposal["packageId"],
            }
            for proposal, outcome in zip(proposals, outcomes)
        ]
        return {
            "messageId": message_id,
            "taskId": task["id"],
            "contextId": task["contextId"],
            "role": "ROLE_USER",
            "parts": [{
                "mediaType": main.RESULTS_MODE,
                "data": {"batchId": "batch-1", "results": results},
            }],
        }

    def test_agent_card_and_authentication(self):
        card_response = self.client.get("/.well-known/agent-card.json")
        self.assertEqual(card_response.status_code, 200)
        card = card_response.json()
        self.assertEqual(card["skills"][0]["id"], "invoice_action_agent")
        self.assertIn(main.INPUT_MODE, card["defaultInputModes"])

        unauthenticated = self.client.get("/a2a/tasks")
        self.assertEqual(unauthenticated.status_code, 401)

    def test_initial_task_and_accepted_receipts_only(self):
        task = self.create_task()
        self.assertEqual(task["status"]["state"], "TASK_STATE_INPUT_REQUIRED")
        self.assertEqual(len(task["artifacts"]), 1)
        self.assertEqual(
            task["artifacts"][0]["parts"][0]["mediaType"], main.PROPOSALS_MODE
        )

        message = self.continuation(task, outcomes=["ACCEPTED", "REJECTED"])
        response = self.client.post(
            "/a2a/message:send", headers=self.headers, json={"message": message}
        )
        self.assertEqual(response.status_code, 200, response.text)
        completed = response.json()["task"]
        self.assertEqual(completed["status"]["state"], "TASK_STATE_COMPLETED")
        receipts = [
            part["data"]
            for artifact in completed["artifacts"]
            for part in artifact.get("parts", [])
            if part["mediaType"] == main.RECEIPTS_MODE
        ]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(len(receipts[0]["executions"]), 1)
        self.assertEqual(
            receipts[0]["executions"][0]["packageId"],
            task["artifacts"][0]["parts"][0]["data"]["proposals"][0]["packageId"],
        )

        replay = self.client.post(
            "/a2a/message:send", headers=self.headers, json={"message": message}
        )
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(replay.json()["task"], completed)

    def test_message_id_conflict_and_user_isolation(self):
        task = self.create_task()
        changed = self.initial_message(message_id="initial-1", package_ids=("different",))
        conflict = self.client.post(
            "/a2a/message:send", headers=self.headers, json={"message": changed}
        )
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json()["error"]["code"], "IDEMPOTENCY_CONFLICT")

        outsider_headers = dict(self.headers, Authorization="Bearer other-user-token")
        outsider_read = self.client.get(f"/a2a/tasks/{task['id']}", headers=outsider_headers)
        self.assertEqual(outsider_read.status_code, 404)
        outsider_list = self.client.get("/a2a/tasks", headers=outsider_headers)
        self.assertEqual(outsider_list.status_code, 200)
        self.assertEqual(outsider_list.json()["tasks"], [])

    def test_cancel_and_receipt_race_has_one_winner(self):
        task = self.create_task()
        message = self.continuation(task)
        barrier = threading.Barrier(2)

        def continue_request():
            barrier.wait()
            return self.client.post(
                "/a2a/message:send", headers=self.headers, json={"message": message}
            )

        def cancel_request():
            barrier.wait()
            return self.client.post(
                f"/a2a/tasks/{task['id']}:cancel", headers=self.headers
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(continue_request)
            b = pool.submit(cancel_request)
            responses = [a.result(), b.result()]

        statuses = sorted(response.status_code for response in responses)
        self.assertEqual(statuses, [200, 409], [r.text for r in responses])
        stored = self.client.get(f"/a2a/tasks/{task['id']}", headers=self.headers)
        self.assertEqual(stored.status_code, 200)
        final_task = stored.json()
        self.assertIn(
            final_task["status"]["state"],
            {"TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"},
        )
        receipt_parts = [
            part
            for artifact in final_task.get("artifacts", [])
            for part in artifact.get("parts", [])
            if part["mediaType"] == main.RECEIPTS_MODE
        ]
        if final_task["status"]["state"] == "TASK_STATE_CANCELED":
            self.assertEqual(receipt_parts, [])
        else:
            self.assertEqual(len(receipt_parts), 1)


if __name__ == "__main__":
    unittest.main()
