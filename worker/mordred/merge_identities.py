import sys
import os
import logging
from datetime import datetime

from sirmordred.config import Config
from sirmordred.task_identities import TaskIdentitiesMerge

import traceback

current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, 'setup.cfg')
JSON_DIR_PATH = 'projects_json'

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("mordred-worker")


def run_mordred():
    print("\n====== Starts (UTC) ======\n{}\n==========================\n".format(datetime.now()))

    cfg = Config(CONFIG_PATH)
    logger.error(cfg.get_conf())
    logger.error(cfg.conf_list)
    result_identities = merge_identities(cfg)

    print("\n====== Finish (UTC) ======\n{}\n==========================\n".format(datetime.now()))

    # Check errors
    if result_identities:
        sys.exit(result_identities)


def merge_identities(config):
    """Execute the merge identities phase
    :param config: a Mordred config object
    """
    print("==>\tMerging identities from Sortinghat")
    task = TaskIdentitiesMerge(config)

    try:
        task.execute()
        print("==>\tIdentities merged")
    except Exception as e:
        logger.error("Error merging identities. Cause: {}".format(e))
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    try:
        run_mordred()
    except Exception:
        traceback.print_exc()
        logger.error("Finished with errors")
        traceback.print_exc()
        sys.exit(1)
