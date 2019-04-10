import time
import logging
import subprocess
import MySQLdb

import config

logging.basicConfig(level=logging.INFO)

DASHBOARD_LOGS = '/dashboard_logs'


class MordredManager:
    def __init__(self, db_config):
        self.conn = MySQLdb.connect(host=db_config['host'], user=db_config['user'], passwd=db_config['password'], db=db_config['name'], port=db_config['port'])
        self.cursor = self.conn.cursor()

    def _db_update(self, query):
        res = self.cursor.execute(query)
        self.conn.commit()
        return res

    def _db_select(self, query):
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        self.conn.commit()
        return row

    def run(self):
        waiting_msg = True
        while True:
            if waiting_msg is True:
                logging.info('Waiting for new repositories...')
                waiting_msg = False

            # Try to get a new repository THIS LOCKS THE ROW
            q = "SELECT id, url_gh, url_git, gh_token_id " \
                "FROM CauldronApp_repository " \
                "WHERE status = 'PENDING' " \
                "ORDER BY last_modified " \
                "LIMIT 1 " \
                "FOR UPDATE;"
            self.cursor.execute(q)
            row = self.cursor.fetchone()
            if row is None:
                # RELEASE THE LOCK
                self.conn.commit()
                time.sleep(1)
                continue

            # We got one, lets update and RELEASE THE LOCK
            id_repo, url_gh, url_git, id_user = row
            logging.info("Analyzing %s", url_gh)
            q = "UPDATE CauldronApp_repository " \
                "SET status = 'RUNNING' " \
                "WHERE id='{}';".format(id_repo)
            self._db_update(q)

            # Get the user token
            q = "SELECT token " \
                "FROM CauldronApp_githubuser " \
                "WHERE id = {};".format(id_user)
            row = self._db_select(q)
            if row is None:
                q = "UPDATE CauldronApp_repository " \
                    "SET status = 'ERROR' " \
                    "WHERE id='{}';".format(id_repo)
                self._db_update(q)
                continue
            token = row[0]

            # Let's run mordred in a command and get the output
            with open('{}/repository_{}.log'.format(DASHBOARD_LOGS, id_repo), 'w') as f_log:
                proc = subprocess.Popen(['python3', '-u', 'mordred/mordred.py', url_gh, url_git, token],
                                        stdout=f_log,
                                        stderr=subprocess.STDOUT)
                proc.wait()
                logging.info("Mordred analysis for {} finished with code: {}".format(url_gh, proc.returncode))

            # Check the output
            if proc.returncode != 0:
                logging.warning('An error occurred while analyzing %s' % url_gh)
                q = "UPDATE CauldronApp_repository SET status = 'ERROR' WHERE id='{}';".format(id_repo)
                self._db_update(q)
            else:
                q = "UPDATE CauldronApp_repository SET status = 'COMPLETED' WHERE id='{}';".format(id_repo)
                self._db_update(q)

            waiting_msg = True




if __name__ == "__main__":
    db = {
        'host': config.DB_HOST,
        'user': config.DB_USER,
        'password': config.DB_PASSWORD,
        'name': config.DB_NAME,
        'port': config.DB_PORT
    }
    manager = MordredManager(db)
    manager.run()
