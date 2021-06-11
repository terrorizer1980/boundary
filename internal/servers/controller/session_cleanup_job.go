package controller

import (
	"context"
	"time"

	"github.com/hashicorp/boundary/internal/errors"
	"github.com/hashicorp/boundary/internal/scheduler"
	"github.com/hashicorp/boundary/internal/servers"
	"github.com/hashicorp/boundary/internal/servers/controller/common"
	"github.com/hashicorp/go-hclog"
)

// sessionCleanupJob defines a periodic job that monitors workers for
// loss of connection and terminates connections on workers that have
// not sent a heartbeat in a significant period of time.
//
// Relevant connections are simply marked as disconnected in the
// database. Connections will be independently terminated by the
// worker, or the event of a synchronization issue between the two,
// the controller will win out and order that the connections be
// closed on the worker.
type sessionCleanupJob struct {
	logger        hclog.Logger
	serversRepoFn common.ServersRepoFactory
	sessionRepoFn common.SessionRepoFactory

	// The amount of time to give disconnected workers before marking
	// their connections as closed.
	statusGracePeriod time.Duration

	// Worker count, which is probably the only visible metric we will
	// be working with. Actual connections closed will be a record
	// count metric that we will probably only log, if anything.
	numProcessed, numWorkers int
}

// newSessionCleanupJob instantiates the session cleanup job.
func newSessionCleanupJob(
	logger hclog.Logger,
	serversRepoFn common.ServersRepoFactory,
	sessionRepoFn common.SessionRepoFactory,
	statusGracePeriod time.Duration,
) (*sessionCleanupJob, error) {
	const op = "controller.newNewSessionCleanupJob"
	switch {
	case logger == nil:
		return nil, errors.New(errors.InvalidParameter, op, "missing logger")
	case serversRepoFn == nil:
		return nil, errors.New(errors.InvalidParameter, op, "missing serversRepoFn")
	case sessionRepoFn == nil:
		return nil, errors.New(errors.InvalidParameter, op, "missing sessionRepoFn")
	}

	return &sessionCleanupJob{
		logger:            logger,
		serversRepoFn:     serversRepoFn,
		sessionRepoFn:     sessionRepoFn,
		statusGracePeriod: statusGracePeriod,
	}, nil
}

// Name returns a short, unique name for the job.
func (j *sessionCleanupJob) Name() string { return "session_cleanup" }

// Description returns the description for the job.
func (j *sessionCleanupJob) Description() string {
	return "Clean up session connections from disconnected workers"
}

// NextRunIn returns the next run time after a job is completed.
//
// The next run time is defined for sessionCleanupJob as one second.
// This is because the job should run continuously to terminate
// connections as soon as a worker has not reported in for a long
// enough time. Only one job will ever run at once, so there is no
// reason why it cannot run again immediately.
func (j *sessionCleanupJob) NextRunIn() (time.Duration, error) { return time.Second, nil }

// Status returns the status of the running job.
func (j *sessionCleanupJob) Status() scheduler.JobStatus {
	return scheduler.JobStatus{
		Completed: j.numProcessed,
		Total:     j.numWorkers,
	}
}

// Run executes the job.
func (j *sessionCleanupJob) Run(ctx context.Context) error {
	const op = "controller.(sessionCleanupJob).Run"
	j.numProcessed = 0
	j.numWorkers = 0

	// Load repos.
	serversRepo, err := j.serversRepoFn()
	if err != nil {
		return errors.Wrap(err, op, errors.WithMsg("error getting servers repo"))
	}

	sessionRepo, err := j.sessionRepoFn()
	if err != nil {
		return errors.Wrap(err, op, errors.WithMsg("error getting session repo"))
	}

	// Fetch all servers, ensure that time is not an issue
	workers, err := serversRepo.ListServers(ctx, servers.ServerTypeWorker, servers.WithLiveness(-1))
	if err != nil {
		return errors.Wrap(err, op, errors.WithMsg("error listing workers"))
	}

	j.numWorkers = len(workers)
	j.logger.Debug(
		"starting job",
		"op", op,
		"num_processed", j.numProcessed,
		"num_workers", j.numWorkers,
	)
	for _, worker := range workers {
		j.logger.Debug(
			"checking worker status",
			"op", op,
			"private_id", worker.PrivateId,
			"num_processed", j.numProcessed,
			"num_workers", j.numWorkers,
		)

		// Increment numProcessed here, as all other states are final.
		j.numProcessed++

		// Check update time against our grace period as a guard.
		isPastGrace, updateTime, gracePeriod := j.isPastGrace(worker.UpdateTime.GetTimestamp().AsTime())
		if !isPastGrace {
			j.logger.Debug(
				"worker status OK, reported within acceptable period",
				"op", op,
				"private_id", worker.PrivateId,
				"update_time", updateTime,
				"grace_period", gracePeriod,
				"num_processed", j.numProcessed,
				"num_workers", j.numWorkers,
			)
			continue
		}

		// If the guard failed, that means that our worker has not
		// reported within the acceptable grace period. Find all active
		// connections for the worker and close them.
		numClosed, err := sessionRepo.CloseDeadConnectionsForWorker(ctx, worker.PrivateId, nil)
		if err != nil {
			// Log errors, this is non-fatal so that we can proceed with
			// the rest of the workers.
			j.logger.Error(
				"error marking connections as closed for worker",
				"op", op,
				"err", err,
				"private_id", worker.PrivateId,
				"num_processed", j.numProcessed,
				"num_workers", j.numWorkers,
			)
			continue
		}

		// Log closed connection message as a warning and continue with
		// the rest of the checks.
		j.logger.Warn(
			"worker has not reported status within acceptable grace period, all connections closed",
			"op", op,
			"private_id", worker.PrivateId,
			"update_time", updateTime,
			"grace_period", gracePeriod,
			"number_connections_closed", numClosed,
			"num_processed", j.numProcessed,
			"num_workers", j.numWorkers,
		)
	}

	j.logger.Debug(
		"job finished",
		"op", op,
		"num_processed", j.numProcessed,
		"num_workers", j.numWorkers,
	)

	return nil
}

func (j *sessionCleanupJob) isPastGrace(updateTime time.Time) (bool, time.Time, time.Duration) {
	grace := j.statusGracePeriod
	elapsed := time.Since(updateTime)
	return elapsed > grace, updateTime, grace
}
