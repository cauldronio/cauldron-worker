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
        if not self.conn:
            self._wait_until_db_ready()

        waiting_msg = True
        while True:
            if waiting_msg:
                self.logger.info('Waiting for new repositories...')
                waiting_msg = False

            # Wait for a task
            task = self._get_task()
            if not task:
                time.sleep(1)
                continue
            task_id, repo_id, user_id = task
            self.logger.info('New task found!')

            # Get the token for the task
            token = self._get_token(user_id)
            if not token:
                self.logger.error("Token for task {} not found".format(task_id))
                self._complete_task(task_id, 'ERROR')

            # Get the repo for the task
            repo = self._get_repo(repo_id)
            if not repo:
                self.logger.error("Repo for task {} not found".format(task_id))
                self._complete_task(task_id, 'ERROR')

            url_gh, url_git = repo
            self.logger.info("Analyzing {}".format(url_gh))
            # Let's run mordred in a command and get the output
            file_logs = '{}/repository_{}.log'.format(DASHBOARD_LOGS, repo_id)
            with open(file_logs, 'w') as f_log:
                proc = subprocess.Popen(['python3', '-u', 'mordred/mordred.py', url_gh, url_git, token],
                                        stdout=f_log,
                                        stderr=subprocess.STDOUT)
                proc.wait()
                self.logger.info("Mordred analysis for {} finished with code: {}".format(url_gh, proc.returncode))

            # Check the output
            if proc.returncode != 0:
                self.logger.error('An error occurred while analyzing %s' % url_gh)
                self._complete_task(task_id, 'ERROR')
            else:
                self._complete_task(task_id, 'COMPLETED')

            waiting_msg = True

    def _get_task(self):
        """
        Try to get a task, this locks the row taken until finish
        :return: Task row
        """
        q = "SELECT id, repository_id, gh_user_id " \
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

    def _get_token(self, user_id):
        """
        Get the user token from the user id
        :param user_id:
        :return:
        """
        q = "SELECT token " \
            "FROM CauldronApp_githubuser " \
            "WHERE id = {};".format(user_id)
        self.cursor.execute(q)
        row = self.cursor.fetchone()
        self.conn.commit()
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
        q = "SELECT repository_id, gh_user_id, worker_id, created, started " \
            "FROM CauldronApp_task " \
            "WHERE id = '{}';".format(task_id)
        self.cursor.execute(q)
        row = self.cursor.fetchone()
        self.conn.commit()
        if not row:
            self.logger.error('Unknown task id to complete: {}'.format(id))
            return
        repo_id, user_id, worker_id, created, started = row

        # Delete the task
        q = "DELETE FROM CauldronApp_task " \
            "WHERE id = '{}';".format(task_id)
        self.cursor.execute(q)
        self.conn.commit()

        # Create completed task
        q = "INSERT INTO CauldronApp_completedtask " \
            "(repository_id, gh_user_id, created, started, completed, status) " \
            "VALUES" \
            "('{}', '{}', '{}', '{}', LOCALTIMESTAMP(), '{}');".format(repo_id, user_id, created, started, status)
        self.cursor.execute(q)
        self.conn.commit()

    def _get_repo(self, repo_id):
        """
        Get the repo information to analyze
        :param repo_id:
        :return:
        """
        q = "SELECT url_gh, url_git " \
            "FROM CauldronApp_repository " \
            "WHERE id = {};".format(repo_id)
        self.cursor.execute(q)
        row = self.cursor.fetchone()
        self.conn.commit()
        return row

    def _wait_until_db_ready(self):
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Wait for projects to be available for analyzing')
    parser.add_argument('--name', type=str, help="Name for the worker", default="")
    args = parser.parse_args()

    manager = MordredManager(args.name)
    manager.run()
