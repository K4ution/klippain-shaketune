"""Regression tests for issue #285; run with python -m unittest discover -s tests -v.

Klipper and numerical dependencies are replaced with test doubles. The writer
tests exercise the real queue/barrier/file logic using a passthrough compressor;
they do not validate Zstandard itself or real printer timing.
"""

import importlib.util
import json
import queue
import sys
import tempfile
import threading
import time
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]


def load_accelerometer():
    # Avoid importing Klipper's plugin entry point in shaketune/__init__.py.
    packages = {}
    for name, path in [('belt_test', ROOT / 'shaketune'), ('belt_test.helpers', ROOT / 'shaketune/helpers')]:
        package = types.ModuleType(name)
        package.__path__ = [str(path)]
        packages[name] = package
    for name in ('numpy', 'zstandard'):
        try:
            __import__(name)
        except ImportError:
            module = types.ModuleType(name)
            if name == 'zstandard':
                module.FLUSH_BLOCK = 0
                module.FLUSH_FRAME = 1
                module.ZstdCompressor = MagicMock()
                module.ZstdDecompressor = MagicMock()
            packages[name] = module
    spec = importlib.util.spec_from_file_location('belt_test.helpers.accelerometer', ROOT / 'shaketune/helpers/accelerometer.py')
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, packages):
        spec.loader.exec_module(module)
    return module


accelerometer_module = load_accelerometer()


class Reactor:
    monotonic = staticmethod(time.monotonic)

    def pause(self, deadline):
        time.sleep(max(0, deadline - time.monotonic()))
        return time.monotonic()


class PassthroughCompressor:
    def __init__(self, **kwargs):
        pass

    def stream_writer(self, stream):
        class Writer:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def write(self, data):
                stream.write(data)

            def flush(self, *args):
                stream.flush()

        return Writer()


class MeasurementFlushTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self.temp.cleanup)
        self.manager = accelerometer_module.MeasurementsManager(10, Reactor(), Path(self.temp.name) / 'belts')

    def start_writer(self):
        self.patch_compressor = patch.object(accelerometer_module, 'ZstdCompressor', PassthroughCompressor)
        self.patch_compressor.start()
        self.addCleanup(self.patch_compressor.stop)
        # A thread lets the test control the writer deterministically on Windows
        # and Linux. Production uses a process; the FIFO barrier is identical.
        self.manager._writer_queue = queue.Queue()
        thread = threading.Thread(
            target=self.manager._writer_loop,
            args=(self.manager._temp_file, self.manager._writer_queue,
                  self.manager._is_writing, self.manager._flush_complete),
            daemon=True,
        )
        self.manager._writer_process = thread
        thread.start()

        def finish():
            self.manager._writer_queue.put(accelerometer_module.STOP_SENTINEL)
            thread.join(2)

        self.addCleanup(finish)

    def test_both_belts_survive_intermediate_flush_in_one_file(self):
        self.start_writer()
        expected = []
        for belt in ('belt_A', 'belt_B'):
            recording = {'name': belt, 'samples': [(0.0, 1.0, 2.0, 3.0)]}
            expected.append(recording)
            self.manager.add_measurement(belt, list(recording['samples']))
            self.manager.flush_completed_measurements()
            self.assertEqual(self.manager.get_measurements(), [])
        self.manager.save_stdata()
        actual = [json.loads(line) for line in self.manager._final_file.read_text().splitlines()]
        self.assertEqual(actual, json.loads(json.dumps(expected)))

    def test_barrier_acknowledges_only_after_serialization(self):
        entered = threading.Event()
        release = threading.Event()
        real_dumps = json.dumps

        def slow_dumps(value):
            entered.set()
            if not release.wait(2):
                raise RuntimeError('test writer was not released')
            return real_dumps(value)

        self.start_writer()
        with patch.object(accelerometer_module.json, 'dumps', slow_dumps):
            self.manager.add_measurement('belt_A', [(0, 1, 2, 3)])
            self.manager._writer_queue.put(self.manager.measurements[0])
            self.manager.clear_measurements()
            self.manager._writer_queue.put(accelerometer_module.FLUSH_SENTINEL)
            try:
                self.assertTrue(entered.wait(2))
                self.assertFalse(self.manager._flush_complete.is_set())
            finally:
                release.set()
            self.assertTrue(self.manager._flush_complete.wait(2))

    def test_timeout_does_not_continue_with_pending_writer(self):
        self.manager._writer_process = MagicMock()
        self.manager._writer_process.is_alive.return_value = True
        with self.assertRaises(TimeoutError):
            self.manager.flush_completed_measurements(timeout=0)

    def test_writer_failure_is_reported(self):
        self.manager._writer_process = MagicMock()
        self.manager._writer_process.is_alive.return_value = False
        with self.assertRaisesRegex(RuntimeError, 'writer exited'):
            self.manager.flush_completed_measurements()

    def test_flush_requires_active_writer_and_reactor(self):
        with self.assertRaises(ValueError):
            self.manager.flush_completed_measurements()

    def test_existing_chunk_flush_keeps_active_recording(self):
        self.manager._writer_queue = queue.Queue()
        self.manager.measurements = [{'name': 'complete', 'samples': []}, {'name': 'active', 'samples': []}]
        self.manager._flush_chunk()
        self.assertEqual(self.manager._writer_queue.get()['name'], 'complete')
        self.assertEqual(self.manager.measurements[0]['name'], 'active')


class BeltCommandTests(unittest.TestCase):
    def test_flush_finishes_before_next_recording_and_graph_runs_after_final_save(self):
        import ast

        source = (ROOT / 'shaketune/commands/compare_belts_responses.py').read_text(encoding='utf-8')
        function = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef))
        events = MagicMock()
        gcmd = MagicMock()
        gcmd.get_float.side_effect = lambda name, default=None, **kwargs: default
        gcmd.get_int.side_effect = lambda name, default=None, **kwargs: default
        gcmd.get.side_effect = lambda name, default=None: default
        toolhead = MagicMock()
        toolhead.get_status.return_value = {'max_accel': 5000, 'minimum_cruise_ratio': 0.5}
        objects = {'toolhead': toolhead, 'gcode': events.gcode, 'input_shaper': events.shaper}
        printer = MagicMock()
        printer.lookup_object.side_effect = lambda name, *args: objects.get(name, MagicMock())
        config = MagicMock()
        config.get_printer.return_value = printer
        compat = MagicMock()
        compat.return_value.get_res_tester_config.return_value = (5, 100, 75, [(150, 150, 20)])
        motors = MagicMock()
        motors.return_value.kinematics = 'corexy'
        accel = MagicMock()
        accel.return_value = events.accel
        manager = MagicMock()
        manager.return_value = events.manager
        process = events.process
        process.get_graph_creator.return_value.get_folder.return_value = Path('/tmp')
        namespace = {
            'datetime': datetime, 'Accelerometer': accel, 'MeasurementsManager': manager,
            'KlipperCompatibility': compat, 'MotorsConfigParser': motors, 'ConsoleOutput': MagicMock(),
            'vibrate_axis': events.vibrate, 'ShakeTuneProcess': object,
            'AXIS_CONFIG': [{'axis': a, 'label': a, 'direction': (1, 1, 0)} for a in ('a', 'b')],
        }
        exec(compile(ast.Module(body=[function], type_ignores=[]), source, 'exec'), namespace)
        namespace['compare_belts_responses'](gcmd, config, process)
        names = [call[0] for call in events.mock_calls]
        starts = [i for i, name in enumerate(names) if name == 'accel.start_recording']
        flushes = [i for i, name in enumerate(names) if name == 'manager.flush_completed_measurements']
        self.assertEqual(len(starts), 2)
        self.assertEqual(len(flushes), 2)
        self.assertLess(starts[0], flushes[0])
        self.assertLess(flushes[0], starts[1])
        self.assertLess(starts[1], flushes[1])
        self.assertLess(flushes[1], names.index('manager.save_stdata'))
        self.assertLess(names.index('manager.save_stdata'), names.index('process.run'))
        events.process.run.assert_called_once()
        events.process.wait_for_completion.assert_called_once()

        # A failed flush must prevent the second recording and restore motion settings.
        events.reset_mock()
        events.manager.flush_completed_measurements.side_effect = TimeoutError('writer stalled')
        with self.assertRaises(TimeoutError):
            namespace['compare_belts_responses'](gcmd, config, process)
        events.accel.start_recording.assert_called_once()
        events.shaper.enable_shaping.assert_called_once()
        self.assertIn('ACCEL=5000 MINIMUM_CRUISE_RATIO=0.5', events.gcode.run_script_from_command.call_args.args[0])
        events.process.run.assert_not_called()


if __name__ == '__main__':
    unittest.main()\n