import os
import time
import datetime
import logging
import subprocess
from contextlib import contextmanager
from functools import wraps

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func
from sqlalchemy.ext.automap import automap_base

import config

# Get the name from an env variable
if 'WORKER_NAME' not in os.environ or not os.environ['WORKER_NAME']:
    raise Exception("WORKER_NAME is not defined for this worker")
WORKER_NAME = os.environ['WORKER_NAME']

# Create the logger
log_format = logging.Formatter("[%(name)s] %(asctime)s [%(levelname)s] %(message)s", "%d-%m-%Y %H:%M:%S")
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.DEBUG)
log_handler.setFormatter(log_format)
Logger = logging.getLogger(WORKER_NAME)
Logger.addHandler(log_handler)
Logger.setLevel(logging.DEBUG)

DASHBOARD_LOGS = '/dashboard_logs'


def retry_func(function, init_delay=1, backoff=2, max_delay=30):
    """
    Retry decorator for the database

    Retries indefinitely the function until no exception is raised using a exponential backoff
    :param function: Function decorated
    :param init_delay: Initial delay in seconds
    :param backoff: Multiplier for the delay
    :param max_delay: Maximum delay in seconds
    :return:
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            delay = init_delay
            is_retry = False
            while True:
                if is_retry:
                    Logger.warning("Retry {}".format(f.__name__))
                try:
                    output = f(*args, **kwargs)
                    if is_retry:
                        Logger.warning("Continue. {} didn't return any error".format(f.__name__))
                    return output
                except:
                    Logger.warning("Retrying in {} seconds".format(delay))
                    time.sleep(delay)
                    delay *= backoff
                    if delay > max_delay:
                        delay = max_delay
                    is_retry = True
        return f_retry
    return deco_retry(function)


class MordredManager:
    def __init__(self):
        self.db_config = {
            'host': config.DB_HOST,
            'user': config.DB_USER,
            'password': config.DB_PASSWORD,
            'name': config.DB_NAME,
            'port': config.DB_PORT
        }

        self.worker_id = WORKER_NAME

        self.models = None
        self.engine = None
        self.Session = None
        self.Base = automap_base()

    def run(self):
        """
        Infinitely wait for tasks and run mordred for the tasks found
        :return:
        """
        self._connect_db()
        self.recovery()
        waiting_msg = True
        while True:
            if waiting_msg:
                Logger.info('Waiting for new tasks...')
                waiting_msg = False

            task = self._get_task()
            if not task:
                time.sleep(1)
                continue
            task_id, repository_id = task
            Logger.info('New task found!')
            self.analyze_task(task_id, repository_id)
            waiting_msg = True

    def analyze_task(self, task_id, repo_id):
        """
        Analyze a full task with mordred
        :param task_id:
        :param repo_id:
        :return:
        """
        # Get the repo for the task
        repo = self._get_repo(repo_id)
        if not repo:
            Logger.error("Repo for task {} not found".format(task_id))
            self._complete_task(task_id, 'ERROR')
            return
        url, backend, index_name = repo
        # Get the token for the task
        token = self._get_token(task_id)
        if not token and backend != 'git':
            Logger.error("Token for task {} not found".format(task_id))
            self._complete_task(task_id, 'ERROR')
            return

        # Update the log location in task object
        file_log = '{}/repo_{}.log'.format(DASHBOARD_LOGS, repo_id)
        self._set_file_log(task_id, file_log)
        
        # Let's run mordred in a command and get the output
        Logger.info("Analyzing [{} | {}]".format(backend, url))
        # TODO: Improve as a function call
        with open(file_log, 'a') as f_log:
            cmd = ['python3', '-u', 'mordred/mordred.py',
                   '--backend', backend,
                   '--url', url,
                   '--index', index_name]
            if backend != 'git':
                cmd.extend(['--token', token])

            proc = subprocess.Popen(cmd, stdout=f_log, stderr=subprocess.STDOUT)
            proc.wait()
            Logger.info("Mordred analysis for [{}|{}] finished with code: {}".format(backend, url, proc.returncode))

        # Check the output
        if proc.returncode == 1:
            Logger.error('An error occurred while analyzing [{}|{}]'.format(backend, url))
            self._complete_task(task_id, 'ERROR')
        elif proc.returncode > 1:
            wait_minutes = proc.returncode
            pending_time = datetime.datetime.now() + datetime.timedelta(minutes=wait_minutes)
            Logger.error('RateLimitError restart at [{}]'.format(pending_time))
            self._set_pending_task(task_id)
            self._update_token_rate_time(token.id, pending_time)
        else:
            self._complete_task(task_id, 'COMPLETED')

    def recovery(self):
        """
        Try to recover tasks with the worker name and analyze them
        :return:
        """
        while True:
            task = self._get_pending_task()
            if task:
                task_id, repository_id = task
                Logger.info("We got a pending task! Try to analyze it again")
                self.analyze_task(task_id, repository_id)
            else:
                Logger.info("No pending tasks")
                break

    @retry_func
    def _get_task(self):
        """
        Try to get a new task
        :return: (task_id, repository_id) or None
        """
        with self._session_scope() as session:
            now = datetime.datetime.now()
            # Get the tasks id with a token ready and randomly selected
            task = session.query(self.models['Task']). \
                join(self.models['Task_Tokens']). \
                join(self.models['Token']).\
                filter(self.models['Token'].rate_time < now).\
                filter(self.models['Task'].worker_id == '').\
                group_by(self.models['Task_Tokens'].token_id).\
                order_by(func.rand()).\
                first()

            if task:
                start_date = task.started if task.started else datetime.datetime.now()
                session.query(self.models['Task']).\
                    filter(self.models['Task'].id == task.id).\
                    update({'worker_id': self.worker_id, 'started': start_date, 'retries': task.retries + 1})

                return task.id, task.repository_id

    @retry_func
    def _get_token(self, task_id):
        """
        Get a token available (Rate limit) for that task
        :param task_id: ID of the task
        :return: Token or None
        """
        with self._session_scope() as session:
            token = session.query(self.models['Token']).\
                filter(self.models['Token'].rate_time < datetime.datetime.now(),
                       self.models['Task_Tokens'].task_id == task_id).\
                first()
            if token:
                return token.key
        return None

    @retry_func
    def _complete_task(self, task_id, status):
        """
        Delete a task and create a completed task with the status parameter
        :param task_id: Task IDto be deleted
        :param status: Final status of the task
        :return:
        """

        # Get the task info
        with self._session_scope() as session:
            task = session.query(self.models['Task']).\
                filter(self.models['Task'].id == task_id).\
                first()

            if not task:
                Logger.error('Unknown task id to complete: {}'.format(id))
                return

            completed = self.models['CompletedTask'](task_id=task.id,
                                                     repository_id=task.repository_id,
                                                     created=task.created,
                                                     started=task.started,
                                                     completed=datetime.datetime.now(),
                                                     retries=task.retries,
                                                     status=status,
                                                     log_file=task.log_file)
            session.add(completed)

            # Delete the task
            session.delete(task)

    @retry_func
    def _set_file_log(self, task_id, file_log):
        """
        Update the log location for this task
        :param task_id: ID of the task
        :param file_log: Absolute path to the logs
        :return:
        """
        with self._session_scope() as session:
            q = session.query(self.models['Task']).\
                filter(self.models['Task'].id == task_id).\
                update({'log_file': file_log})

    @retry_func
    def _get_repo(self, repo_id):
        """
        Get the repository information for analyzing
        :param repo_id: ID of the repository in the DB
        :return: (url, backend, index_name) or None
        """
        with self._session_scope() as session:
            repo = session.query(self.models['Repository']).\
                filter(self.models['Repository'].id == repo_id).\
                first()
            if repo:
                return repo.url, repo.backend, repo.index_name
        return None

    def _connect_db(self):
        """
        This block until can connect to the database and the task-table is available
        :return:
        """
        self.engine = create_engine("mysql://{}:{}@{}/{}".format(
            self.db_config['user'],
            self.db_config['password'],
            self.db_config['host'],
            self.db_config['name']
        ))

        while True:
            Logger.info("Trying to connect to the database")
            try:
                self.Base.prepare(self.engine, reflect=True)
                self._load_models()
                self.Session = sessionmaker(bind=self.engine)
                with self._session_scope() as session:
                    session.query(self.Base.classes.CauldronApp_task)
                break
            except (sqlalchemy.exc.OperationalError, AttributeError) as e:
                Logger.warning("Error from database. {}".format(e.args[0]))
                Logger.info("RETRY IN 2 SECONDS...")
                time.sleep(2)
                continue

        Logger.info("We are ready!")

    def _load_models(self):
        self.models = dict()
        self.models['AnonymousUser'] = self.Base.classes.CauldronApp_anonymoususer
        self.models['CompletedTask'] = self.Base.classes.CauldronApp_completedtask
        self.models['Dashboard'] = self.Base.classes.CauldronApp_dashboard
        self.models['ESUser'] = self.Base.classes.CauldronApp_esuser
        self.models['GithubUser'] = self.Base.classes.CauldronApp_githubuser
        self.models['GitlabUser'] = self.Base.classes.CauldronApp_gitlabuser
        self.models['Repository'] = self.Base.classes.CauldronApp_repository
        self.models['Repository_Dashboards'] = self.Base.classes.CauldronApp_repository_dashboards
        self.models['Task'] = self.Base.classes.CauldronApp_task
        self.models['Task_Tokens'] = self.Base.classes.CauldronApp_task_tokens
        self.models['Token'] = self.Base.classes.CauldronApp_token
        self.models['User'] = self.Base.classes.auth_user

    @retry_func
    def _get_pending_task(self):
        """
        Check if the worker has pending tasks (Maybe because got down)
        :return: (id, repository_id) or None
        """
        with self._session_scope() as session:
            task = session.query(self.models['Task']).\
                filter(self.models['Task'].worker_id == self.worker_id).\
                order_by(self.models['Task'].created).\
                first()
            if task:
                return task.id, task.repository_id
            return None

    @retry_func
    def _set_pending_task(self, task_id):
        """
        It removes the worker_id and leave the task
        :param task_id: ID of the task
        :return:
        """
        with self._session_scope() as session:
            q = session.query(self.models['Task']).\
                filter(self.models['Task'].id == task_id).\
                update({'worker_id': ''})

    @retry_func
    def _update_token_rate_time(self, token_id, rate_time):
        """
        Update the time when the token will be available again (Reached rate limit)
        :param token_id: ID of the token in the DB
        :param rate_time: Time when the token will be available again
        :return:
        """
        with self._session_scope() as session:
            q = session.query(self.models['Token']).\
                filter(self.models['Token'].id == token_id).\
                update({'rate_time': rate_time})

    @contextmanager
    def _session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            Logger.warning("Rollback the last session {}".format(e.args[0]))
            session.rollback()
            raise
        finally:
            session.close()


if __name__ == "__main__":
    manager = MordredManager()
    manager.run()
