# Mordred worker


> This project is not intended to be executed from outside the Cauldron network and without the container provided in cauldronio/cauldron-deployment. All the libraries specified in `requirements.txt` could not be enough for running.


This worker accomplish the following tasks:
- Connect to the Django database (db_cauldron) and keep the connection stable. If it looses the connection, try to reconnect. The queries are in transactions.
- The worker gets all the tasks with `worker_id` = `$WORKER_NAME` (environment variable).
- If there is none with that name, the worker gets one with the following rules:
    - Tasks that haven't got a `worker_id`.
    - Tasks that have an available token (did not reach the rate limit)
    - Group the tasks by token (one task per token)
    - Select one randomly
- When the worker has a task, it retrieves all the information related (url, backend and token)
- The worker starts a new process running `grimoirelab sirmordred` with a custom configuration.
- Once `sirmordred` has finished, the worker save the status in the database and starts again getting a new task.


### Development
For deploying a live-version of a worker and see the changes in your code, you will need to make some adjustments in your configuration:

- First, you will need to clone this repository into your local machine:

  ```bash
  $ git clone https://gitlab.com/cauldronio/cauldron-worker.git
  ```

- Next, you will need to modify the file `<deployment_path>/playbooks/roles/run_cauldron/tasks/run_mordred.yml` from the [deployment repository](https://gitlab.com/cauldronio/cauldron-deployment), adding the next line at the end of the `volumes` section:

  ```bash
  - "<cauldron_path>:/code/cauldron-worker"
  ```

- The next time you run Cauldron, every change made to your local version of this repository will overwrite the one located in the container of the deployment repository.
