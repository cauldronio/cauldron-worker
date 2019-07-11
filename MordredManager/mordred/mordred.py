import sys
import json
import tempfile
import os
import logging
import argparse
import math
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

from sirmordred.config import Config
from sirmordred.task_collection import TaskRawDataCollection
from sirmordred.task_enrich import TaskEnrich
from sirmordred.task_projects import TaskProjects

import sqlalchemy
import ssl
from elasticsearch.connection import create_ssl_context

# logging.basicConfig(level=logging.INFO)

CONFIG_PATH = 'mordred/setup-default.cfg'
JSON_DIR_PATH = 'projects_json'


def run_mordred(backend, url, token, index_name):
    print("\n====== Starts (UTC) ======\n{}\n==========================\n".format(datetime.now()))
    projects_file = _create_projects_file(backend, url)
    cfg = _create_config(projects_file, backend, token, index_name)
    _get_raw(cfg, backend)
    _get_enrich(cfg, backend)
    _update_aliases(cfg)


def _update_aliases(cfg):
    # TODO: Verify SSL Elasticsearch
    conf = cfg.get_conf()
    context = create_ssl_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    es = Elasticsearch([conf['es_enrichment']['url']], timeout=100, use_ssl=True, ssl_context=context)

    put_alias_no_except(es, index='git_aoc_enriched_*', name='git_aoc_enriched')
    put_alias_no_except(es, index='git_enrich_*', name='git_enrich')
    #put_alias_no_except(es, index='git_raw_*', name='git_raw')

    put_alias_no_except(es, index='github_enrich_*', name='github_enrich')
    #put_alias_no_except(es, index='github_raw_*', name='github_raw')

    #put_alias_no_except(es, index='gitlab_raw_*', name='gitlab_raw')
    put_alias_no_except(es, index='gitlab_enriched_*', name='gitlab_enriched')

    put_alias_no_except(es, index='git_enrich_*', name='ocean')
    put_alias_no_except(es, index='github_enrich_*', name='ocean')
    put_alias_no_except(es, index='gitlab_enriched_*', name='ocean')

    put_alias_no_except(es, index='github_enrich_*', name='ocean_tickets')
    put_alias_no_except(es, index='gitlab_enriched_*', name='ocean_tickets')


def put_alias_no_except(es, index, name):
    try:
        es.indices.put_alias(index=index, name=name)
    except NotFoundError:
        pass


def _create_projects_file(backend, url):
    """
    Check the backend of the repository and create the projects.json for it
    :param backend: gitlab, github, git ...
    :param url: url for the repository
    :return:
    """
    logging.info("Creating projects.json for {}: {}".format(backend, url))
    if not os.path.isdir(JSON_DIR_PATH):
        try:
            os.mkdir(JSON_DIR_PATH)
        except OSError:
            logging.error("Creation of directory %s failed", JSON_DIR_PATH)
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


def _create_config(projects_file, backend, token, index_name):
    cfg = Config(CONFIG_PATH)
    if backend != 'git':
        cfg.set_param(backend, 'api-token', token)
    cfg.set_param('projects', 'projects_file', projects_file)
    cfg.set_param('git', 'raw_index', "git_raw_{}".format(index_name))
    cfg.set_param('git', 'enriched_index', "git_enrich_{}".format(index_name))
    cfg.set_param('github', 'raw_index', "github_raw_{}".format(index_name))
    cfg.set_param('github', 'enriched_index', "github_enrich_{}".format(index_name))
    cfg.set_param('github:issue', 'raw_index', "github_issues_raw_{}".format(index_name))
    cfg.set_param('github:issue', 'enriched_index', "github_issues_enriched_{}".format(index_name))
    cfg.set_param('github:pull', 'raw_index', "github_pulls_raw_{}".format(index_name))
    cfg.set_param('github:pull', 'enriched_index', "github_pulls_enriched_{}".format(index_name))
    cfg.set_param('enrich_areas_of_code:git', 'in_index', "git_raw_{}".format(index_name))
    cfg.set_param('enrich_areas_of_code:git', 'out_index', "git_aoc_enriched_{}".format(index_name))
    cfg.set_param('enrich_onion:git', 'in_index', "git_enriched_{}".format(index_name))
    cfg.set_param('enrich_onion:git', 'out_index', "git_onion_enriched_{}".format(index_name))
    cfg.set_param('enrich_onion:github', 'in_index_iss', "github_issues_enriched_{}".format(index_name))
    cfg.set_param('enrich_onion:github', 'in_index_prs', "github_pulls_enriched_{}".format(index_name))
    cfg.set_param('enrich_onion:github', 'out_index_iss', "github_issues_onion_enriched_{}".format(index_name))
    cfg.set_param('enrich_onion:github', 'out_index_prs', "github_prs_onion_enriched_{}".format(index_name))
    cfg.set_param('gitlab', 'raw_index', "gitlab_raw_{}".format(index_name))
    cfg.set_param('gitlab', 'enriched_index', "gitlab_enriched_{}".format(index_name))

    return cfg


def _get_raw(config, backend):
    logging.info("Loading raw data for %s", backend)
    TaskProjects(config).execute()
    # I am not using arthur
    task = TaskRawDataCollection(config, backend_section=backend)
    try:
        repositories = task.execute()
        if len(repositories) != 1:
            logging.error("Critical error: More than 1 repository found in the output")
        repo = repositories[0]
        if 'error' in repo and repo['error']:
            if repo['error'].startswith('RateLimitError'):
                restart_minutes = math.ceil(float(repo['error'].split(' ')[-1])/60) + 10
                logging.warning("RateLimitError. This task will be restarted in: {} minutes".format(restart_minutes))
                sys.exit(restart_minutes)
            else:
                logging.error(repo['error'])
                sys.exit(1)

        logging.info("Loading raw data for %s finished!", backend)
    except Exception as e:
        logging.warning("Error loading raw data from {}. Raising exception. Cause: {}".format(backend, e))
        sys.exit(1)


def _get_enrich(config, backend):
    logging.info("Enriching data for %s", backend)
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
        logging.info("Data for %s enriched!", backend)
    except Exception as e:
        logging.warning("Error enriching data for %s. Raising exception", backend)
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run mordred for a repository")
    parser.add_argument('--backend', type=str, help='Backend to analyze')
    parser.add_argument('--url', type=str, help='URL repository to analyze')
    parser.add_argument('--token', type=str, help='token for the analysis', default="")
    parser.add_argument('--index', type=str, help='index for ES')
    args = parser.parse_args()
    try:
        run_mordred(args.backend, args.url, args.token, args.index)
    except Exception:
        logging.error("Finished with errors")
        sys.exit(1)
