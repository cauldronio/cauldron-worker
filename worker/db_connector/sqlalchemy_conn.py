from contextlib import contextmanager
from functools import wraps

import sqlalchemy
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

import logging
import time
import datetime

logger = logging.getLogger(__name__)

BACKENDS_WITH_TOKEN = ('github', 'gitlab', 'meetup')


def retry_func(function, init_delay=1, backoff=2, max_delay=20, max_attempts=10):
    """ Retry decorator for the database"""
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            delay = init_delay
            attempts = 1
            is_retry = False
            while True:
                if is_retry and logger:
                    logger.warning("Retry {}".format(f.__name__))
                try:
                    output = f(*args, **kwargs)
                    if is_retry and logger:
                        logger.info("Continue. {} didn't return any error after the retry".format(f.__name__))
                    return output
                except Exception as exc:
                    if logger:
                        logger.error("Error in {}: {}".format(f.__name__, exc.args[0]))
                        logger.error("Retrying in {} seconds ({}/{})".format(delay, attempts, max_attempts))
                    time.sleep(delay)
                    delay *= backoff
                    attempts += 1
                    if attempts > max_attempts:
                        raise Exception("The worker is in a bad state stopping...")
                    if delay > max_delay:
                        delay = max_delay
                    is_retry = True
        return f_retry
    return deco_retry(function)


class SQLAlchemyConn:
    def __init__(self, host, port, db_name, user, password):
        self.db_config = {
            'host': host,
            'user': user,
            'password': password,
            'name': db_name,
            'port': port
        }
        self.Base = automap_base()
        self.models = None
        self.engine = None
        self.Session = None

    def connect_db(self):
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
            logger.info("Trying to connect to the database")
            try:
                self.Base.prepare(self.engine, reflect=True)
                self._load_models()
                self.Session = sessionmaker(bind=self.engine)
                with self._session_scope() as session:
                    session.query(self.Base.classes.CauldronApp_task)
                break
            except (sqlalchemy.exc.OperationalError, AttributeError) as e:
                logger.warning("Error from database. {}".format(e.args[0]))
                logger.info("RETRY IN 2 SECONDS...")
                time.sleep(10)
                continue

        logger.info("We are ready!")

    @retry_func
    def get_assigned_task(self, name):
        """
        Check if the worker has pending tasks (Maybe because got down)
        :return: (task_id, repository_id, token_id, token_key) or None
        """
        with self._session_scope() as session:
            tasks = session.query(self.models['Task']).\
                filter(self.models['Task'].worker_id == name).\
                order_by(self.models['Task'].created)
            task = tasks.first()
            if not task:
                return

            repo = self.get_repo(task.repository_id)
            if not repo:
                logger.error("Repo for task {} not found".format(task.id))
                self.finish_task(task.id, 'ERROR')
                return

            repo_id, repo_backend = repo
            if repo_backend in BACKENDS_WITH_TOKEN:
                token = self.get_token_available(task.id)
                if token:
                    token_id, token_key = token
                else:
                    self.set_pending_task(task.id)
                    return
            else:
                token_id, token_key = None, None
            task_id, task_repository_id = task.id, task.repository_id
        return task_id, task_repository_id, token_id, token_key

    @retry_func
    def get_available_task(self, worker_name):
        """
        Try to get a new task
        :return: (task_id, repository_id, token_id, token_key) or None
        """
        with self._session_scope() as session:
            now = datetime.datetime.now()
            # Get the tasks id with a token ready and randomly selected
            tasks_tokens = session.query(self.models['Task'], self.models['Token']). \
                outerjoin(self.models['Task_Tokens'], self.models['Task_Tokens'].task_id == self.models['Task'].id). \
                outerjoin(self.models['Token'], self.models['Token'].id == self.models['Task_Tokens'].token_id).\
                with_for_update()
            # Do not remove: We need to lock the table with this line and with "with_for_update()"
            tasks_tokens.all()
            # Filter not valid tokens
            tokens_in_use_q = tasks_tokens.filter(self.models['Task'].worker_id != '')\
                .distinct(self.models['Token'].id)\
                .with_entities(self.models['Token'].id)
            tokens_in_use = [token_id for token_id, in tokens_in_use_q if token_id is not None]

            valid_tasks = tasks_tokens.filter(sqlalchemy.or_(
                self.models['Token'].rate_time < now,
                self.models['Token'].rate_time == None)
            ).filter(sqlalchemy.or_(
                self.models['Token'].id.notin_(tokens_in_use),
                self.models['Token'].id == None)
            ).filter(
                self.models['Task'].worker_id == ''
            )
            task_and_token = valid_tasks.group_by(self.models['Task_Tokens'].token_id)\
                .order_by(func.rand())\
                .first()
            if task_and_token:
                task = task_and_token[0]
                token = task_and_token[1]
                start_date = task.started if task.started else datetime.datetime.now()
                session.query(self.models['Task']).\
                    filter(self.models['Task'].id == task.id).\
                    update({'worker_id': worker_name, 'started': start_date, 'retries': task.retries + 1})

                task_id, repository_id = task.id, task.repository_id
                if token:
                    token_id, token_key = token.id, token.key
                else:
                    token_id, token_key = None, None

                return task_id, repository_id, token_id, token_key

    @retry_func
    def get_repo(self, repo_id):
        """Get the url and backend of a repository"""
        with self._session_scope() as session:
            repo = session.query(self.models['Repository']).\
                filter(self.models['Repository'].id == repo_id).\
                first()
            if repo:
                return repo.url, repo.backend
        return None

    @retry_func
    def finish_task(self, task_id, status):
        """
        Delete a task and create a completed task with the status parameter
        :param task_id: Task ID to be deleted
        :param status: Final status of the task
        :return:
        """

        with self._session_scope() as session:
            task = session.query(self.models['Task']).\
                filter(self.models['Task'].id == task_id).\
                first()
            if not task:
                logger.error('Unknown task id to complete: {}'.format(task_id))
                return

            completed = self.models['CompletedTask'](task_id=task.id,
                                                     repository_id=task.repository_id,
                                                     created=task.created,
                                                     started=task.started,
                                                     completed=datetime.datetime.now(),
                                                     retries=task.retries,
                                                     status=status,
                                                     log_file=task.log_file,
                                                     old=False)
            session.add(completed)
            session.delete(task)

    @retry_func
    def assign_log_file(self, task_id, filename):
        """Update the log location for a task"""
        with self._session_scope() as session:
            session.query(self.models['Task']).\
                filter(self.models['Task'].id == task_id).\
                update({'log_file': filename})

    @retry_func
    def set_pending_task(self, task_id):
        """Remove the worker_id and leave the task as pending"""
        with self._session_scope() as session:
            session.query(self.models['Task']).\
                filter(self.models['Task'].id == task_id).\
                update({'worker_id': ''})

    @retry_func
    def update_token_rate_time(self, token_id, rate_time):
        """Update the time when the token will be available again (Reached rate limit)"""
        with self._session_scope() as session:
            session.query(self.models['Token']).\
                filter(self.models['Token'].id == token_id).\
                update({'rate_time': rate_time})

    @retry_func
    def get_token_available(self, task_id):
        """Get a token available (Rate limit) for that task"""
        with self._session_scope() as session:
            tokens = session.query(self.models['Token'])\
                .join(self.models['Task_Tokens'], self.models['Task_Tokens'].token_id == self.models['Token'].id)\
                .filter(self.models['Task_Tokens'].task_id == task_id)\
                .filter(self.models['Token'].rate_time < datetime.datetime.now())
            token = tokens.first()

            if token:
                return token.id, token.key
        return None

    @retry_func
    def sh_task_ready(self):
        now = datetime.datetime.utcnow()
        with self._session_scope() as session:
            q = session.query(self.models['SHTask'])\
                .filter(self.models['SHTask'].done.is_(False))\
                .filter(self.models['SHTask'].scheduled_date < now)
            ready = session.query(q.exists()).scalar()
            logger.debug('SH Task ready: {}'.format(ready))
        return ready

    @retry_func
    def get_sh_task(self):
        task_id, task_sched, task_logfile = None, None, None
        with self._session_scope() as session:
            sh_task = session.query(self.models['SHTask'])\
                .filter(self.models['SHTask'].done.is_(False)).first()
            if sh_task:
                task_id, task_sched, task_logfile = sh_task.id, sh_task.scheduled_date , sh_task.log_file
        return task_id, task_sched, task_logfile

    @retry_func
    def create_sh_task(self, scheduled, log_file):
        task_id, task_sched, task_logfile = None, None, None
        with self._session_scope() as session:
            sh_task = self.models['SHTask'](scheduled_date=scheduled,
                                            done=False,
                                            log_file=log_file)
            session.add(sh_task)
            task_id, task_sched, task_logfile = sh_task.id, sh_task.scheduled_date, sh_task.log_file
        return task_id, task_sched, task_logfile

    @retry_func
    def are_running_tasks(self):
        with self._session_scope() as session:
            q = session.query(self.models['Task'])\
                .filter(self.models['Task'].worker_id != '')
            running = session.query(q.exists()).scalar()
        return running

    @retry_func
    def start_sh_task(self, task_id):
        now = datetime.datetime.utcnow()
        with self._session_scope() as session:
            session.query(self.models['SHTask']).\
                filter(self.models['SHTask'].id == task_id).\
                update({'started_date': now,
                        'done': True})

    @retry_func
    def complete_sh_task(self, task_id):
        now = datetime.datetime.utcnow()
        with self._session_scope() as session:
            session.query(self.models['SHTask']).\
                filter(self.models['SHTask'].id == task_id).\
                update({'completed_date': now,
                        'done': True})

    def _load_models(self):
        self.models = dict()
        self.models['AnonymousUser'] = self.Base.classes.CauldronApp_anonymoususer
        self.models['CompletedTask'] = self.Base.classes.CauldronApp_completedtask
        self.models['Dashboard'] = self.Base.classes.CauldronApp_dashboard
        self.models['GithubUser'] = self.Base.classes.CauldronApp_githubuser
        self.models['GitlabUser'] = self.Base.classes.CauldronApp_gitlabuser
        self.models['MeetupUser'] = self.Base.classes.CauldronApp_meetupuser
        self.models['Repository'] = self.Base.classes.CauldronApp_repository
        self.models['Repository_Dashboards'] = self.Base.classes.CauldronApp_repository_dashboards
        self.models['Task'] = self.Base.classes.CauldronApp_task
        self.models['Task_Tokens'] = self.Base.classes.CauldronApp_task_tokens
        self.models['Token'] = self.Base.classes.CauldronApp_token
        self.models['User'] = self.Base.classes.auth_user
        self.models['SHTask'] = self.Base.classes.CauldronApp_shtask

    @contextmanager
    def _session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            logger.warning("Rollback the last session: {}".format(e.args[0]))
            session.rollback()
            raise
        finally:
            session.close()
