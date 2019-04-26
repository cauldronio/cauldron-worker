import time
import logging
import subprocess
import MySQLdb

import config
import argparse

# CREATE A LOGGER CONFIG
log_format = logging.Formatter("[%(name)s] %(asctime)s [%(levelname)s] %(message)s", "%d-%m-%Y %H:%M:%S")
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.DEBUG)
log_handler.setFormatter(log_format)

DASHBOARD_LOGS = '/dashboard_logs'


class MordredManager:
    def __init__(self, name=None):
        self.db_config = {
            'host': config.DB_HOST,
            'user': config.DB_USER,
            'password': config.DB_PASSWORD,
            'name': config.DB_NAME,
            'port': config.DB_PORT
        }

        self.worker_id = name or config.WORKER_NAME
        self.conn = None
        self.cursor = None

        self.logger = logging.getLogger(self.worker_id)
        self.logger.addHandler(log_handler)
        self.logger.setLevel(logging.DEBUG)

    def run(self):
        """
        Infinitely wait for tasks and run mordred for the tasks found
        :return:
        """
        self._wait_until_db_ready()
        self.recovery()
        waiting_msg = True
        while True:
            if waiting_msg:
                self.logger.info('Waiting for new tasks...')
                waiting_msg = False

            task = self._get_task()
            if not task:
                time.sleep(1)
                continue
            self.logger.info('New task found!')
            task_id, repo_id, user_id = task
            self.analyze_task(task_id, repo_id, user_id)
            waiting_msg = True

    def analyze_task(self, task_id, repo_id, user_id):
        """
        Analyze a full task with mordred
        :param task_id:
        :param repo_id:
        :param user_id:
        :return:
        """
        # Get the repo for the task
        repo = self._get_repo(repo_id)
        if not repo:
            self.logger.error("Repo for task {} not found".format(task_id))
            self._complete_task(task_id, 'ERROR')
            return

        url, backend, index_name = repo

        # Get the token for the task
        token = self._get_token(backend, user_id)
        if not token and backend != 'git':
            self.logger.error("Token for task {} not found".format(task_id))
            self._complete_task(task_id, 'ERROR')
            return

        # Update the log location in task object
        file_log = '{}/task_{}.log'.format(DASHBOARD_LOGS, task_id)
        self._set_file_log(task_id, file_log)
        
        # Let's run mordred in a command and get the output
        self.logger.info("Analyzing [{} | {}]".format(backend, url))
        with open(file_log, 'w') as f_log:
            cmd = ['python3', '-u', 'mordred/mordred.py',
                   '--backend', backend,
                   '--url', url,
                   '--index', index_name]
            if backend != 'git':
                cmd.extend(['--token', token])

            proc = subprocess.Popen(cmd, stdout=f_log, stderr=subprocess.STDOUT)
            proc.wait()
            self.logger.info("Mordred analysis for [{}|{}] finished with code: {}".format(backend, url, proc.returncode))

        # Check the output
        if proc.returncode != 0:
            self.logger.error('An error occurred while analyzing [{}|{}]'.format(backend, url))
            self._complete_task(task_id, 'ERROR')
            return
        else:
            self._complete_task(task_id, 'COMPLETED')
            return

    def recovery(self):
        """
        Try to recover task with the worker name and analyze them
        :return:
        """
        while True:
            task = self._get_pending_task()
            if task:
                task_id, repo_id, user_id = task
                self.analyze_task(task_id, repo_id, user_id)
            else:
                self._complete_task(task_id, 'COMPLETED')

    def recovery(self):
        """
        Try to recover task with the worker name and analyze them
        :return:
        """
        while True:
            task = self._get_pending_task()
            if task:
                task_id, repo_id, user_id = task
                self.analyze_task(task_id, repo_id, user_id)
            else:
                break

    def _get_task(self):
        """
        Try to get a task, this locks the row taken until finish
        :return: Task row
        """
        q = "SELECT id, repository_id, user_id " \
            "FROM CauldronApp_task " \
            "WHERE worker_id = '' " \
            "ORDER BY created " \
            "LIMIT 1 " \
            "FOR UPDATE;"
        self.cursor.execute(q)
        row = self.cursor.fetchone()
        if row is None:
            # RELEASE THE LOCK
            self.conn.commit()
            return None

        # We got one, lets update fast and RELEASE THE LOCK
        task_id, repo_id, user_id = row
        q = "UPDATE CauldronApp_task " \
            "SET worker_id = '{}', started = LOCALTIMESTAMP() " \
            "WHERE repository_id='{}';".format(self.worker_id, repo_id)
        self.cursor.execute(q)
        self.conn.commit()

        return task_id, repo_id, user_id

    def _get_token(self, backend, user_id):
        """
        Get the user token from the user id
        :param backend: github, gitlab, git...
        :param user_id: User id from django
        :return:
        """
        if backend == 'git':
            return None
        elif backend == 'github':
            q = "SELECT token " \
                "FROM CauldronApp_githubuser " \
                "WHERE user_id = {};".format(user_id)
            self.cursor.execute(q)
            row = self.cursor.fetchone()
            self.conn.commit()
        elif backend == 'gitlab':
            q = "SELECT token " \
                "FROM CauldronApp_gitlabuser " \
                "WHERE user_id = {};".format(user_id)
            self.cursor.execute(q)
            row = self.cursor.fetchone()
            self.conn.commit()
        else:
            row = None

        if row:
            row = row[0]
        return row

    def _complete_task(self, task_id, status):
        """
        Delete a task and create a completed task with the status parameter
        :param task_id: Task IDto be deleted
        :param status: Final status of the task
        :return:
        """

        # Get the task info
        q = "SELECT repository_id, user_id, worker_id, created, started, log_file " \
            "FROM CauldronApp_task " \
            "WHERE id = '{}';".format(task_id)
        self.cursor.execute(q)
        row = self.cursor.fetchone()
        self.conn.commit()
        if not row:
            self.logger.error('Unknown task id to complete: {}'.format(id))
            return
        repo_id, user_id, worker_id, created, started, log_file = row

        # Delete the task
        q = "DELETE FROM CauldronApp_task " \
            "WHERE id = '{}';".format(task_id)
        self.cursor.execute(q)
        self.conn.commit()

        # Create CompletedTask
        q = "INSERT INTO CauldronApp_completedtask " \
            "(task_id, repository_id, user_id, created, started, completed, status, log_file) " \
            "VALUES" \
            "('{}', '{}', '{}', '{}', '{}', LOCALTIMESTAMP(), '{}', '{}');".format(task_id, repo_id, user_id, created, started, status, log_file)
        self.cursor.execute(q)
        self.conn.commit()

    def _set_file_log(self, task_id, file_log):
        """
        Update the log location for this task
        :param task_id: ID of the task
        :param file_log: Absolute path to the logs
        :return:
        """
        q = "UPDATE CauldronApp_task " \
            "SET log_file = '{}' " \
            "WHERE id='{}';".format(file_log, task_id)
        self.cursor.execute(q)
        self.conn.commit()

    def _get_repo(self, repo_id):
        """
        Get the repo information to analyze
        :param repo_id:
        :return:
        """
        q = "SELECT url, backend, index_name " \
            "FROM CauldronApp_repository " \
            "WHERE id = {};".format(repo_id)
        self.cursor.execute(q)
        row = self.cursor.fetchone()
        self.conn.commit()
        return row

    def _wait_until_db_ready(self):
        """
        This block until can connect to the database and the task-table is available
        :return:
        """
        while not self.conn:
            self.logger.info("Trying to connect to the database")
            try:
                self.conn = MySQLdb.connect(host=self.db_config['host'],
                                            user=self.db_config['user'],
                                            passwd=self.db_config['password'],
                                            db=self.db_config['name'],
                                            port=self.db_config['port'])
            except MySQLdb.OperationalError as e:
                self.logger.warning("Error from database. code[{}] {}".format(e.args[0], e.args[1]))
                self.logger.info("RETRY IN 2 SECONDS...")
                time.sleep(2)
        self.logger.info('We have the connection! Wait until the tables are created...')
        self.cursor = self.conn.cursor()
        ready = False
        while not ready:
            # Just to check the database is ready
            q = "SELECT * " \
                "FROM CauldronApp_task " \
                "WHERE worker_id = '' " \
                "LIMIT 1;"
            try:
                self.cursor.execute(q)
                self.conn.commit()
                ready = True
            except (MySQLdb.OperationalError, MySQLdb.ProgrammingError) as e:
                self.logger.warning("Error from database. code[{}] {}".format(e.args[0], e.args[1]))
                self.logger.info("RETRY IN 2 SECONDS...")
                time.sleep(2)
        self.logger.info("We are ready!")

    def _get_pending_task(self):
        """
        Check if I the worker has pending tasks (Maybe because got down)
        :return:
        """
        q = "SELECT id, repository_id, user_id " \
            "FROM CauldronApp_task " \
            "WHERE worker_id = '{}' " \
            "ORDER BY created " \
            "LIMIT 1;".format(self.worker_id)
        self.cursor.execute(q)
        row = self.cursor.fetchone()
        self.conn.commit()
        if row is None:
            self.logger.info("No pending tasks")
        else:
            self.logger.info("We got a pending task! Try to analyze it again")
            # Update the task start time
            task_id, repo_id, user_id = row
            q = "UPDATE CauldronApp_task " \
                "SET started = LOCALTIMESTAMP() " \
                "WHERE repository_id='{}';".format(repo_id)
            self.cursor.execute(q)
        return row


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Wait for projects to be available for analyzing')
    parser.add_argument('--name', type=str, help="Name for the worker", default="")
    args = parser.parse_args()

    manager = MordredManager(args.name)
    manager.run()
