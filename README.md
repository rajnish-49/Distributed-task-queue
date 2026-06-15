# Distributed Task Queue

A small background job queue built with Go and PostgreSQL.

The API accepts jobs and stores them in Postgres. A separate worker process polls for pending jobs, claims one safely, runs the matching handler, and updates the job status. Built to understand how job queues work under the hood before adding a separate broker like Redis or RabbitMQ.

## Features

- `POST /jobs` to create a background job
- `GET /jobs/{id}` to check job status
- PostgreSQL-backed job storage
- Separate API and worker binaries
- Safe job claiming with `FOR UPDATE SKIP LOCKED`
- Retry scheduling with `scheduled_at`
- Terminal `dead` state after max attempts

## Tech Stack

- Go
- `net/http`
- PostgreSQL
- `pgx`
- Goose migrations

## Architecture

![Architecture diagram](docs/architecture.png)

The project has two runnable programs:

- `cmd/api` starts the HTTP API.
- `cmd/worker` starts the worker process.

PostgreSQL is used as both the job store and the queue. Jobs live in a `jobs` table, and the worker claims pending rows inside a transaction.

## Job Lifecycle

A job starts as `pending`. When the worker claims it, the job moves to `processing` and the attempt count is incremented. If the handler succeeds, the job becomes `completed`. If the handler fails, the job is either scheduled again as `pending` or marked `dead` after it runs out of attempts.

The claim step uses row locking, so multiple workers can poll at the same time without picking the same job.

## Project Structure

```txt
cmd/
  api/        HTTP API entry point
  worker/     Worker process entry point

internal/
  config/     Environment configuration
  http/       HTTP handlers
  job/        Job model and statuses
  migrations/ Database migrations
  storage/    PostgreSQL queries and job state changes
  worker/     Worker loop and job execution logic
```

## API Endpoints

| Method | Endpoint     | Description       |
| ------ | ------------ | ----------------- |
| POST   | `/jobs`      | Create a new job  |
| GET    | `/jobs/{id}` | Fetch a job by ID |

### `POST /jobs`

Request body:

```json
{
  "type": "send_email",
  "payload": {
    "to": "test@example.com"
  },
  "max_attempts": 3
}
```

### `GET /jobs/{id}`

Returns the persisted job state, including status, attempts, payload, and timestamps.

## Running Locally

You need Go, PostgreSQL, and Goose installed.

Install Goose if needed:

```powershell
go install github.com/pressly/goose/v3/cmd/goose@latest
```

Create a `.env` file:

```env
DATABASE_URL=postgresql://user:password@host/database?sslmode=require
```

Run migrations:

```powershell
goose -dir internal/migrations postgres "$env:DATABASE_URL" up
```

Start the API in one terminal:

```powershell
go run ./cmd/api
```

Start the worker in another terminal:

```powershell
go run ./cmd/worker
```

## Try It

With the API and worker running, create a job:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8080/jobs `
  -ContentType "application/json" `
  -Body '{"type":"send_email","payload":{"to":"test@example.com"},"max_attempts":3}'
```

Then fetch the job by ID:

```powershell
Invoke-RestMethod http://localhost:8080/jobs/1
```

Use the job ID returned from the create request.

## Worker Behavior

The worker claim query uses:

```sql
FOR UPDATE SKIP LOCKED
```

This prevents multiple workers from processing the same pending job at the same time.

The current worker has one registered handler, `send_email`, which logs the payload and completes the job. Failed handlers are retried with a basic time-based backoff.

## Demo

### Create a Job

![Create job request](docs/screenshots/create-job.png)

### Worker Processes the Job

![Worker processing logs](docs/screenshots/worker-processing.png)

### Fetch Completed Job

![Fetch completed job](docs/screenshots/get-completed-job.png)

## Current Limitations

Not covered yet:

- no dashboard
- no automated tests yet
- no recovery for jobs stuck in `processing` after a worker crash
- job handlers are hardcoded in the worker
- PostgreSQL is doing queue duty for now

