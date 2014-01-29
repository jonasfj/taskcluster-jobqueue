import os
import unittest
import subprocess
import sys
import time

class TestJobQueueMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.python = 'python{}.{}'.format(sys.version_info.major, sys.version_info.minor)
        cls.jobqueue_path = None
        paths = ['src/jobqueue.py', '../src/jobqueue.py']
        for path in paths:
            if os.path.isfile(path):
                cls.jobqueue_path = path
                break

    def test_basic_startup(self):

        self.assertIsNotNone(TestJobQueueMain.jobqueue_path)
        cmd = [TestJobQueueMain.python, TestJobQueueMain.jobqueue_path]
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)

        # TODO: find nicer way than using sleep
        time.sleep(2)
        proc.terminate()
        proc.wait()
        err = proc.stderr.read()
        proc.stderr.close()
        if len(err):
            print(err)
        self.assertEqual(len(err), 0)
        self.assertNotEqual(proc.returncode, 1)

    # TODO: add tests for command line arguments

if __name__ == '__main__':
    unittest.main()
