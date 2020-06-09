# Grimoirelab Worker


> This project is not intended to be executed from outside the Cauldron network and without the container provided in cauldronio/cauldron-deployment. All the libraries specified in `requirements.txt` could not be enough for running outside the provided Dockerfile

This worker has 2 modes:
- Analyze GitHub, Gitlab, Git and Meetup repositories.
- Execute MergeIdentities everyday at a specific time.

## Analyze repositories
- Connect to the Django database (db_cauldron) and keep the connection stable. If it looses the connection, try to reconnect. The queries are in transactions.
- If there is a [merge identities task](#merge-identities) running, wait until finish.
- The worker gets all the tasks with `worker_id` = `$WORKER_NAME`
- If there is none with that name, the worker gets one with the following rules:
    - Tasks that haven't got a `worker_id`.
    - Tasks that have an available token (didn't reach the rate limit and it's not used by other)
    - Group the tasks by token (one task per token)
    - Select one randomly
- When the worker has a task, it starts a new process running `grimoirelab sirmordred` with a custom configuration.
- Once `sirmordred` has finished, the worker save the status in the database and starts again.

## Merge identities
> IMPORTANT. There must be only 1 worker running in this mode
- Connect to the Django database (db_cauldron) and keep the connection stable. If it looses the connection, try to reconnect. The queries are in transactions.
- The worker tries to obtain an existing SortingHat task if it doesn't exist, creates a new one and schedule it for a configured time
- When the time comes, wait until there aren't repositories being analyzed.
- It starts a new process running sirmordred with only Merge Identities task.
- Once `sirmordred` has finished, the worker mark the task as completed and go back to step 2.

## Development
For deploying a live-version of a worker and see the changes in your code, you will need to run [cauldron-deployment](https://gitlab.com/cauldronio/cauldron-deployment) with the following configuration: [Development environment](https://gitlab.com/cauldronio/cauldron-deployment#development-environment)

## Docker

### Build
You can build an image for this repository with Ansible: [cauldron deployment (build images)](https://gitlab.com/cauldronio/cauldron-deployment#advanced-deployment-for-cauldron)

Or you can run from the root of this repository:

```bash
docker build -f docker/Dockerfile -t cauldronio/worker:test .
```

### Run
You can run the image for this repository with Ansible: [cauldron deployment (run Cauldron)](https://gitlab.com/cauldronio/cauldron-deployment#advanced-deployment-for-cauldron)

Or with the image previously created:

```bash
docker run --rm -e "DB_HOST=aaa" -e "DB_USER=bbb" -e "DB_PASSWORD=ccc" -e "DB_NAME=ddd" -e "WORKER_NAME=worker_0" cauldron-worker:test --repository-tasks
```

Two options in the end:
- `--repository-tasks`
- `--identities-merge SCHEDULED_HOUR`