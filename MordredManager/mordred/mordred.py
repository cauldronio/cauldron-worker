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
from sirmordred.task_identities import TaskIdentitiesMerge
from sirmordred.task_projects import TaskProjects

import sqlalchemy
import traceback

CONFIG_PATH = 'mordred/setup-default.cfg'
JSON_DIR_PATH = 'projects_json'

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("mordred-worker")


def run_mordred(backend, url, token, git_path=None):
    print("\n====== Starts (UTC) ======\n{}\n==========================\n".format(datetime.now()))
    projects_file = create_projects_file(backend, url)
    cfg = create_config(projects_file, backend, token, git_path)
    result_raw = get_raw(cfg, backend)
    if cfg.get_conf()['phases']['identities']:
        result_identities = merge_identities(cfg)
    else:
        result_identities = None
    result_enrich = get_enrich(cfg, backend)
    print("\n====== Finish (UTC) ======\n{}\n==========================\n".format(datetime.now()))

    # Check errors
    if result_raw:
        sys.exit(result_raw)
    if result_enrich:
        sys.exit(result_enrich)
    if result_identities:
        sys.exit(result_identities)


def create_projects_file(backend, url):
    """
    Check the backend of the repository and create the projects.json for it
    :param backend: gitlab, github, meetup, git ...
    :param url: url for the repository
    :return:
    """
    logger.info("Creating projects.json for {}: {}".format(backend, url))
    if not os.path.isdir(JSON_DIR_PATH):
        try:
            os.mkdir(JSON_DIR_PATH)
        except OSError:
            logger.error("Creation of directory %s failed", JSON_DIR_PATH)
    projects = dict()
    projects['Project'] = dict()
    projects['Project'][backend] = list()
    projects['Project'][backend].append(url)

    projects_file = tempfile.NamedTemporaryFile('w+',
                                                prefix='projects_',
                                                dir=JSON_DIR_PATH,
                                                delete=False)
    json.dump(projects, projects_file)

    return projects_file.name


def create_config(projects_file, backend, token, git_path=None):
    cfg = Config(CONFIG_PATH)
    if backend != 'git':
        cfg.set_param(backend, 'api-token', token)
    cfg.set_param('projects', 'projects_file', projects_file)
    # GIT
    cfg.set_param('git', 'raw_index', "git_raw_index")
    cfg.set_param('git', 'enriched_index', "git_enrich_index")
    if git_path:
        cfg.set_param('git', 'git-path', git_path)
    cfg.set_param('enrich_areas_of_code:git', 'in_index', "git_raw_index")
    cfg.set_param('enrich_areas_of_code:git', 'out_index', "git_aoc_enriched_index")
    # GITHUB
    cfg.set_param('github', 'raw_index', "github_raw_index")
    cfg.set_param('github', 'enriched_index', "github_enrich_index")
    # GITLAB
    cfg.set_param('gitlab', 'raw_index', "gitlab_raw_index")
    cfg.set_param('gitlab', 'enriched_index', "gitlab_enriched_index")
    # MEETUP
    cfg.set_param('meetup', 'raw_index', "meetup_raw_index")
    cfg.set_param('meetup', 'enriched_index', "meetup_enriched_index")

    return cfg


def get_raw(config, backend):
    """
    Execute the collection of raw data. If a exception is occurred it is returned
    :param config:
    :param backend:
    :return: None if everything was ok, 1 for fail, other for minutes to restart
    """
    logger.info("Loading raw data for %s", backend)
    TaskProjects(config).execute()
    task = TaskRawDataCollection(config, backend_section=backend)
    try:
        repositories = task.execute()
        if len(repositories) != 1:
            logger.error("Critical error: More than 1 repository found in the output")
        repo = repositories[0]
        if 'error' in repo and repo['error']:
            if repo['error'].startswith('RateLimitError'):
                seconds_to_reset = float(repo['error'].split(' ')[-1])
                restart_minutes = math.ceil(seconds_to_reset/60) + 1
                logger.warning("RateLimitError. This task will be restarted in: {} minutes".format(restart_minutes))
                return restart_minutes
            else:
                logger.error(repo['error'])
                return 1

        logger.info("Loading raw data for %s finished!", backend)
        return None
    except Exception as e:
        logger.warning("Error loading raw data from {}. Cause: {}".format(backend, e))
        traceback.print_exc()
        return 1


def merge_identities(config):
    """Execute the merge identities phase
    :param config: a Mordred config object
    """
    print("Merging identities from Sortinghat")
    TaskProjects(config).execute()
    task = TaskIdentitiesMerge(config)
    try:
        task.execute()
        print("Identities merged")
    except Exception as e:
        logger.error("Error merging identities. Cause: {}".format(e))
        traceback.print_exc()
        return 1


def get_enrich(config, backend):
    print("Enriching data for {}".format(backend))
    TaskProjects(config).execute()
    task = None
    while not task:
        try:
            task = TaskEnrich(config, backend_section=backend)
        except sqlalchemy.exc.InternalError:
            # There is a race condition in the code
            task = None

    try:
        task.execute()
        print("Data enriched for {}".format(backend))
    except Exception as e:
        logger.warning("Error enriching data for %s. Raising exception", backend)
        traceback.print_exc()
        return 1
    return None


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
