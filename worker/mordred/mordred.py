import sys
import json
import tempfile
import os
import logging
import argparse
import math
from datetime import datetime

from sirmordred.config import Config
from sirmordred.task_collection import TaskRawDataCollection
from sirmordred.task_enrich import TaskEnrich
from sirmordred.task_projects import TaskProjects

import sqlalchemy
import traceback

CONFIG_PATH = 'mordred/setup.cfg'
JSON_DIR_PATH = 'projects_json'

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("mordred-worker")


def run_mordred(backend, url, token, git_path=None):
    print("\n====== Starts (UTC) ======\n{}\n==========================\n".format(datetime.now()))

    projects_file = create_projects_file(backend, url)
    cfg = create_config(projects_file, backend, token, git_path)

    result_raw = get_raw(cfg)
    result_enrich = get_enrich(cfg)

    print("\n====== Finish (UTC) ======\n{}\n==========================\n".format(datetime.now()))

    # Check errors
    if result_raw:
        sys.exit(result_raw)
    if result_enrich:
        sys.exit(result_enrich)


def create_projects_file(backend, url):
    """
    Check the backend of the repository and create the projects.json for it
    :param backend: git, gitlab, github or meetup
    :param url: url for the repository
    :return:
    """
    logger.info("Creating projects.json for {}: {}".format(backend, url))
    if not os.path.isdir(JSON_DIR_PATH):
        try:
            os.mkdir(JSON_DIR_PATH)
        except OSError:
            logger.error("Creation of directory %s failed", JSON_DIR_PATH)
            raise
    projects = {'Project': {}}
    if backend == 'git':
        projects['Project']['git'] = [url]
    elif backend == 'github':
        projects['Project']['github:issue'] = [url]
        # projects['Project']['github:pull'] = [url]
        projects['Project']['github:repo'] = [url]
        projects['Project']['github2:issue'] = [url]
        # projects['Project']['github2:pull'] = [url]
    elif backend == 'gitlab':
        projects['Project']['gitlab:issue'] = [url]
        projects['Project']['gitlab:merge'] = [url]
    elif backend == 'meetup':
        projects['Project']['meetup'] = [url]
    else:
        raise Exception('Unknown backend {}'.format(backend))

    projects_file = tempfile.NamedTemporaryFile('w+',
                                                prefix='projects_',
                                                dir=JSON_DIR_PATH,
                                                delete=False)
    json.dump(projects, projects_file)

    return projects_file.name


def create_config(projects_file, backend, token=None, git_path=None):
    cfg = Config(CONFIG_PATH)
    if backend == 'git' and git_path:
        cfg.set_param('git', 'git-path', git_path)
    elif backend == 'github':
        cfg.set_param('github:issue', 'api-token', token)
        # cfg.set_param('github:pull', 'api-token', token)
        cfg.set_param('github:repo', 'api-token', token)
        cfg.set_param('github2:issue', 'api-token', token)
        # cfg.set_param('github2:pull', 'api-token', token)
    elif backend == 'gitlab':
        cfg.set_param('gitlab:issue', 'api-token', token)
        cfg.set_param('gitlab:merge', 'api-token', token)
    elif backend == 'meetup':
        cfg.set_param('meetup', 'api-token', token)
    else:
        raise Exception('Unknown backend {}'.format(backend))
    cfg.set_param('projects', 'projects_file', projects_file)

    return cfg


def get_raw(config):
    """
    Execute the collection of raw data. If a exception is occurred it is returned
    :param config:
    :return: None if everything was ok, 1 for fail, other for minutes to restart
    """
    TaskProjects(config).execute()
    backend_sections = _get_backend_sections()
    for backend in backend_sections:
        print("==>\tStart raw data retrieval from {}".format(backend))

        task = TaskRawDataCollection(config, backend_section=backend)

        try:
            output_repos = task.execute()
            if len(output_repos) != 1:
                logger.error("More than 1 repository found in the output")
            repo = output_repos[0]
            if 'error' in repo and repo['error']:
                logger.error(repo['error'])
                if repo['error'].startswith('RateLimitError'):
                    seconds_to_reset = float(repo['error'].split(' ')[-1])
                    restart_minutes = math.ceil(seconds_to_reset/60) + 2
                    logger.warning("RateLimitError. This task will be restarted in: {} minutes".format(restart_minutes))
                    return restart_minutes

        except Exception as e:
            logger.error("Error in raw data retrieval from {}. Cause: {}".format(backend, e))
            traceback.print_exc()
            return 1


def _get_backend_sections():
    """
    Return a list of the backend sections defined in project.json
    :return:
    """
    projects = TaskProjects.get_projects()
    if len(projects.keys()) != 1:
        raise Exception('More than one project found. Not allowed. {}'.format(projects))
    for pro in projects:
        return list(projects[pro])


def get_enrich(config):
    TaskProjects(config).execute()
    backend_sections = _get_backend_sections()
    for backend in backend_sections:
        print("==>\tStart enrich for {}".format(backend))
        task = None
        while not task:
            try:
                task = TaskEnrich(config, backend_section=backend)
            except sqlalchemy.exc.InternalError:
                # There is a race condition in the code
                task = None

        try:
            task.execute()
        except Exception as e:
            logger.warning("Error enriching data for %s. Raising exception", backend)
            traceback.print_exc()
            return 1


def config_logging():
    """
    Config logging level output
    """
    logging_levels = {
        'CRITICAL': logging.CRITICAL,
        'FATAL': logging.FATAL,
        'ERROR': logging.ERROR,
        'WARN': logging.WARNING,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG
    }
    level_env = os.getenv('LOG_LEVEL', '')
    level = logging_levels.get(level_env, logging.WARNING)
    logger.setLevel(level)


def get_params():
    """Get params to execute mordred"""
    parser = argparse.ArgumentParser(description="Run mordred for a repository")
    parser.add_argument('--backend', type=str, help='Backend to analyze')
    parser.add_argument('--url', type=str, help='URL repository to analyze')
    parser.add_argument('--token', type=str, help='token for the analysis', default="")
    parser.add_argument('--git-path', dest='git_path', type=str,
                        help='path where the Git repository will be cloned', default=None)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_params()
    config_logging()

    try:
        run_mordred(args.backend, args.url, args.token, args.git_path)
    except Exception:
        traceback.print_exc()
        logger.error("Finished with errors")
        traceback.print_exc()
        sys.exit(1)
