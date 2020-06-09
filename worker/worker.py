import os
import time
import datetime
import logging
import random
import subprocess
import argparse

import traceback

from db_connector.sqlalchemy_conn import SQLAlchemyConn
import config

# Create the logger
log_format = logging.Formatter("[%(name)s] %(asctime)s [%(levelname)s] %(message)s", "%d-%m-%Y %H:%M:%S")
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.DEBUG)
log_handler.setFormatter(log_format)
Logger = logging.getLogger(config.WORKER_NAME)
Logger.addHandler(log_handler)
Logger.setLevel(logging.DEBUG)

DASHBOARD_LOGS = '/dashboard_logs'
SORTINGHAT_LOGS = '/merge_identities_logs'
PERCEVAL_GITPATH = '/git-perceval'
BACKENDS_WITH_TOKEN = ('github', 'gitlab', 'meetup')


class TasksWorker:
    def __init__(self, name):
        self.worker_id = name
        self.api = SQLAlchemyConn(host=config.DB_HOST,
                                  port=config.DB_PORT,
                                  db_name=config.DB_NAME,
                                  user=config.DB_USER,
                                  password=config.DB_PASSWORD)

    def run(self):
        """Infinitely wait for tasks and run mordred for the tasks found"""
        self.api.connect_db()
        waiting_msg = True
        while True:
            if waiting_msg:
                Logger.info('Waiting for new tasks...')

            # First analyze assigned tasks
            task = self.api.get_assigned_task(self.worker_id)
            if task:
                task_id, repository_id, token_id, token_key = task
                Logger.info("We got a task assigned to the worker! Try to analyze it again")
                self.analyze_task(task_id, repository_id, token_id, token_key)
                continue

            self._wait_identities_task()

            task = self.api.get_available_task(self.worker_id)
            if task:
                task_id, repository_id, token_id, token_key = task
                Logger.info('New task found!')
                self.analyze_task(task_id, repository_id, token_id, token_key)
                waiting_msg = True

            time.sleep(10)
            continue

    def analyze_task(self, task_id, repo_id, token_id, token_key):
        """Analyze a task with mordred"""
        repo = self.api.get_repo(repo_id)
        if not repo:
            Logger.error("Repo for task {} not found".format(task_id))
            self.api.finish_task(task_id, 'ERROR')
            return

        url, backend = repo
        file_log = "{}/repo_{}.log".format(DASHBOARD_LOGS, repo_id)
        self.api.assign_log_file(task_id, file_log)

        # Let's run mordred in a command and get the output
        Logger.info("Analyzing [{} | {}]".format(backend, url))
        # TODO: Improve as a function call
        with open(file_log, 'a') as f_log:
            cmd = ['python3', '-u', 'mordred/mordred.py',
                   '--backend', backend,
                   '--url', url]
            if backend == 'git':
                processed_uri = url.lstrip('/')
                git_path = os.path.join(PERCEVAL_GITPATH, processed_uri) + '-git'
                cmd.extend(['--git-path', git_path])
            if backend in BACKENDS_WITH_TOKEN:
                if token_key:
                    cmd.extend(['--token', token_key])
                else:
                    Logger.error("Key not found for task {}. Set as pending".format(task_id))
                    self.api.set_pending_task(task_id)
                    # Sleep some seconds to avoid this error again
                    time.sleep(random.random()*5)
                    return

            proc = subprocess.Popen(cmd, stdout=f_log, stderr=subprocess.STDOUT)
            proc.wait()
            Logger.info("Mordred analysis for [{}|{}] finished with code: {}".format(backend, url, proc.returncode))

        # Check the output
        if proc.returncode == 1:
            Logger.error('An error occurred while analyzing [{}|{}]'.format(backend, url))
            self.api.finish_task(task_id, 'ERROR')
        elif proc.returncode > 1:
            wait_minutes = proc.returncode
            pending_time = datetime.datetime.now() + datetime.timedelta(minutes=wait_minutes)
            Logger.error('RateLimitError restart at [{}]'.format(pending_time))
            if backend in BACKENDS_WITH_TOKEN:
                self.api.update_token_rate_time(token_id, pending_time)
            self.api.set_pending_task(task_id)
        else:
            self.api.finish_task(task_id, 'COMPLETED')

    def _wait_identities_task(self):
        while self.api.sh_task_ready():
            Logger.info('Waiting for IdentitiesMerge to finish')
            time.sleep(10)


class SortingHatWorker:
    def __init__(self, name, scheduled_time):
        self.worker_id = name
        self.scheduled_time = datetime.datetime.strptime(scheduled_time, '%H:%M')
        self.api = SQLAlchemyConn(host=config.DB_HOST,
                                  port=config.DB_PORT,
                                  db_name=config.DB_NAME,
                                  user=config.DB_USER,
                                  password=config.DB_PASSWORD)

    def run(self):
        """Schedule a MergeIdentities task from the database
        at the defined time or get one if exists.

        Wait until task.datetime > now and all tasks finished.

        Execute MergeIdentities.

        Schedule a task for the next iteration
        """
        self.api.connect_db()
        while True:
            task_id, task_scheduled, task_logfile = self.api.get_sh_task()
            if not task_id:
                now = datetime.datetime.utcnow()
                scheduled = now.replace(hour=self.scheduled_time.hour, minute=self.scheduled_time.minute, second=0)
                if scheduled < now:
                    scheduled += datetime.timedelta(days=1)
                log_file = "{}/{}.log".format(SORTINGHAT_LOGS, scheduled.strftime('%Y-%m-%d_%H-%M-%S'))
                task_id, task_scheduled, task_logfile = self.api.create_sh_task(scheduled, log_file)
            self._sleep(until=task_scheduled)
            self._wait_no_running()
            self.api.start_sh_task(task_id)
            self.merge_identities(task_logfile)
            self.api.complete_sh_task(task_id)

    def merge_identities(self, log_file):
        Logger.info('Start merge identities task')
        with open(log_file, 'a') as f_log:
            cmd = ['python3', '-u', 'mordred/merge_identities.py']
            proc = subprocess.Popen(cmd, stdout=f_log, stderr=f_log)
            proc.wait()
        Logger.info("Merge identities finished with code: {}".format(proc.returncode))

    def _sleep(self, until):
        """Sleep until defined datetime in a logarithmic approach."""
        Logger.info('Wait until {}'.format(until))
        while True:
            diff = (until - datetime.datetime.utcnow()).total_seconds()

            if diff < 0:
                break
            time.sleep(diff/2)

    def _wait_no_running(self):
        """Wait until no enrichment/raw tasks are running"""
        while self.api.are_running_tasks():
            Logger.info("Wait for enrich/raw tasks to finish")
            time.sleep(5)


def get_params():
    """Get params to execute the worker"""
    parser = argparse.ArgumentParser(description="Run a worker to perform Grimoire tasks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--repository-tasks', action='store_true',
                       help='Get Repository tasks and analyze them')
    group.add_argument('--identities-merge', metavar='sched_time', type=str,
                       help="Execute IdentitiesMerge daily at the defined time")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_params()
    if args.repository_tasks:
        worker = TasksWorker(config.WORKER_NAME)
    else:
        worker = SortingHatWorker(config.WORKER_NAME, args.identities_merge)

    while True:
        try:
            worker.run()
        except Exception as e:
            traceback.print_exc()
            Logger.error("Critical error: {}".format(e.args[0]))
            Logger.error("Restarting the worker in 5 seconds...")
            time.sleep(5)
