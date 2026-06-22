import unittest
import sys
import threading
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))

from report_concurrency import SectionPassExecutor


class SectionPassExecutorTest(unittest.TestCase):
    def test_recoverable_failure_retries_only_failed_item_and_enters_cooldown(self):
        class CapacityError(Exception):
            status_code = 503

        now = [100.0]
        attempts = {"a": 0, "b": 0}

        def worker(item):
            attempts[item] += 1
            if item == "b" and attempts[item] == 1:
                raise CapacityError("busy")
            return f"result-{item}"

        executor = SectionPassExecutor(
            max_parallel=2,
            cooldown_seconds=60,
            clock=lambda: now[0],
        )

        self.assertEqual(
            executor.run(["a", "b"], worker),
            ["result-a", "result-b"],
        )
        self.assertEqual(attempts, {"a": 1, "b": 2})
        self.assertEqual(executor.effective_parallelism(), 1)

        now[0] = 161.0
        self.assertEqual(executor.effective_parallelism(), 2)

    def test_nonrecoverable_failure_is_not_retried(self):
        attempts = 0

        def worker(_item):
            nonlocal attempts
            attempts += 1
            raise ValueError("invalid response")

        executor = SectionPassExecutor(max_parallel=2)

        with self.assertRaises(ValueError):
            executor.run(["a", "b"], worker)

        self.assertEqual(attempts, 2)

    def test_parallel_execution_overlaps_work_and_preserves_input_order(self):
        barrier = threading.Barrier(2)
        second_finished = threading.Event()

        def worker(item):
            barrier.wait(timeout=1)
            if item == "a":
                self.assertTrue(second_finished.wait(timeout=1))
            else:
                second_finished.set()
            return f"result-{item}"

        executor = SectionPassExecutor(max_parallel=2, cooldown_seconds=60)

        self.assertEqual(
            executor.run(["a", "b"], worker),
            ["result-a", "result-b"],
        )

    def test_sequential_mode_never_overlaps_work(self):
        active = 0
        peak = 0

        def worker(item):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            active -= 1
            return item

        executor = SectionPassExecutor(max_parallel=1)

        self.assertEqual(executor.run([1, 2, 3], worker), [1, 2, 3])
        self.assertEqual(peak, 1)

    def test_falsey_callable_clock_is_preserved(self):
        class FalseyClock:
            def __bool__(self):
                return False

            def __call__(self):
                return 123.0

        clock = FalseyClock()
        executor = SectionPassExecutor(clock=clock)
        self.assertIs(executor.clock, clock)

    def test_max_parallel_is_clamped_to_at_least_one(self):
        executor = SectionPassExecutor(max_parallel=0, cooldown_seconds=60)
        self.assertEqual(executor.max_parallel, 1)

    def test_cooldown_seconds_is_clamped_to_non_negative(self):
        executor = SectionPassExecutor(max_parallel=2, cooldown_seconds=-1)
        self.assertEqual(executor.cooldown_seconds, 0)


class ReportGeneratorConcurrencyTest(unittest.TestCase):
    def test_falsey_model_client_factory_is_preserved(self):
        from report_generation import ReportGenerator

        class FalseyFactory:
            def __bool__(self):
                return False

            def __call__(self):
                raise AssertionError("factory should not be called during construction")

        factory = FalseyFactory()
        generator = ReportGenerator(None, model_client_factory=factory)

        self.assertIs(generator.model_client_factory, factory)

    def test_report_generator_preserves_section_pass_order(self):
        from report_generation import ReportGenerator

        generator = ReportGenerator(None)
        generator.SECTION_PASSES = (
            {"label": "first", "sections": ("A",), "include_visuals": False},
            {"label": "second", "sections": ("B",), "include_visuals": True},
        )
        generator._run_generation_pass = lambda *args: args[-1]

        generated = generator._generate_sections({}, "", "", "")

        self.assertEqual(generated, ["first", "second"])

    def test_report_generator_uses_isolated_client_per_generation_pass(self):
        from report_generation import ReportGenerator

        clients_created = []

        class FakeClient:
            def chat(self, **_kwargs):
                return {
                    "done_reason": "stop",
                    "message": {"content": "# Section"},
                    "eval_count": 2,
                }

        def client_factory():
            client = FakeClient()
            clients_created.append(client)
            return client

        generator = ReportGenerator(None, model_client_factory=client_factory)
        generator._build_report_prompt = lambda *args: "system"
        generator._build_user_instruction = lambda *args: "user"

        generator._run_generation_pass({}, "", "", "", ("A",), False, "a")
        generator._run_generation_pass({}, "", "", "", ("B",), False, "b")

        self.assertEqual(len(clients_created), 2)


if __name__ == "__main__":
    unittest.main()
