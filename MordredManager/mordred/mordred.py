# import sys
import json
import tempfile
import os
import logging
import argparse
from elasticsearch import Elasticsearch

from urllib.parse import urlparse

from sirmordred.config import Config
from sirmordred.task_collection import TaskRawDataCollection
from sirmordred.task_enrich import TaskEnrich
from sirmordred.task_projects import TaskProjects
from sirmordred.task_panels import TaskPanels, TaskPanelsMenu

# logging.basicConfig(level=logging.INFO)

CONFIG_PATH = 'mordred/setup-default.cfg'
JSON_DIR_PATH = 'projects_json'
BACKEND_SECTIONS = ['git', 'github']


def run_mordred(repo_gh, repo_git, gh_token):
    projects_file = _create_projects_file(repo_gh, repo_git)
    index_name = _repo_name(repo_gh)
    cfg = _create_config(projects_file, gh_token, index_name)
    _get_raw(cfg)
    _get_enrich(cfg)
    _update_aliases(cfg)
    # _get_panels(cfg)


def _update_aliases(cfg):
    # TODO: Verify SSL Elasticsearch
    conf = cfg.get_conf()
    es = Elasticsearch([conf['es_enrichment']['url']], timeout=100, verify_certs=False, use_ssl=True)
    es.indices.put_alias(index='git_aoc_enriched_*', name='git_aoc_enriched')
    es.indices.put_alias(index='git_enrich_*', name='git_enrich')
    es.indices.put_alias(index='git_raw_*', name='git_raw')
    es.indices.put_alias(index='github_enrich_*', name='github_enrich')
    es.indices.put_alias(index='github_raw_*', name='github_raw')


def _repo_name(gh_url):
    o = urlparse(gh_url)
    owner, repo = o.path.split('/')[1:]
    return "{}_{}".format(owner, repo).lower()


def _create_projects_file(repo_gh, repo_git):
    """
    Check the source of the repository and create the projects.json for it
    :param repo: URL for the repository
    :return: Path for the projects.json
    """
    logging.info("Creating projects.json for %s", repo_gh)
    if not os.path.isdir(JSON_DIR_PATH):
        try:
            os.mkdir(JSON_DIR_PATH)
        except OSError:
            logging.error("Creation of directory %s failed", JSON_DIR_PATH)
    projects = dict()
    projects['Project'] = dict()
    projects['Project']['git'] = list()
    projects['Project']['git'].append(repo_git)
    projects['Project']['github'] = list()
    projects['Project']['github'].append(repo_gh)

    projects_file = tempfile.NamedTemporaryFile('w+',
                                                prefix='projects_',
                                                dir=JSON_DIR_PATH,
                                                delete=False)
    json.dump(projects, projects_file)

    return projects_file.name


def _create_config(projects_file, gh_token, index_name):
    cfg = Config(CONFIG_PATH)
    cfg.set_param('github', 'api-token', gh_token)
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

    return cfg


def _get_raw(config):
    logging.info("Loading raw data...")
    for backend in BACKEND_SECTIONS:
        logging.info("Loading raw data for %s", backend)
        TaskProjects(config).execute()  # Basically get the projects and save them in the TaskProjects
        task = TaskRawDataCollection(config, backend_section=backend)
        try:
            task.execute()
            logging.info("Loading raw data for %s finished!", backend)
        except Exception as e:
            logging.warning("Error loading raw data from %s. Raising exception", backend)
            raise


def _get_enrich(config):
    logging.info("Enriching data...")
    for backend in BACKEND_SECTIONS:
        logging.info("Enriching data for %s", backend)
        TaskProjects(config).execute()
        task = TaskEnrich(config, backend_section=backend)
        try:
            task.execute()
            logging.info("Data for %s enriched!", backend)
        except Exception as e:
            logging.warning("Error enriching data for %s. Raising exception", backend)
            raise


def _get_panels(config):
    task = TaskPanels(config)
    task.execute()

    task = TaskPanelsMenu(config)
    task.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run mordred for a repository")
    parser.add_argument('repo_gh', help='Github repository to analyze')
    parser.add_argument('repo_git', help='Git repository to analyze')
    parser.add_argument('key', help='key for GitHub')
    args = parser.parse_args()
    run_mordred(args.repo_gh, args.repo_git, args.key)
