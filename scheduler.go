// Package scheduler is a small library that you can use within your application that enables you to execute callbacks (goroutines) after a pre-defined amount of time. GTS also provides task storage which is used to invoke callbacks for tasks which couldn’t be executed during down-time as well as maintaining a history of the callbacks that got executed.
package scheduler

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sapiens-sapide/scheduler/storage"
	"github.com/sapiens-sapide/scheduler/task"
)

// Scheduler is used to schedule tasks. It holds information about those tasks
// including metadata such as argument types and schedule times
type Scheduler struct {
	funcRegistry *task.FuncRegistry
	stopChan     chan bool
	tasks        map[task.ID]*task.Task
	taskStore    storeBridge
}

// New will return a new instance of the Scheduler struct.
func New(store storage.TaskStore) Scheduler {
	funcRegistry := task.NewFuncRegistry()
	return Scheduler{
		funcRegistry: funcRegistry,
		stopChan:     make(chan bool),
		tasks:        make(map[task.ID]*task.Task),
		taskStore: storeBridge{
			store:        store,
			funcRegistry: funcRegistry,
		},
	}
}

// RunAt will schedule function to be executed once at the given time.
func (scheduler *Scheduler) RunAt(time time.Time, function task.Function, params ...task.Param) (task.ID, error) {
	funcMeta, err := scheduler.funcRegistry.Add(function)
	if err != nil {
		return "", err
	}

	tsk := task.New(funcMeta, params)

	tsk.NextRun = time

	scheduler.registerTask(tsk, true)
	return tsk.Hash(), nil
}

// RunAfter executes function once after a specific duration has elapsed.
func (scheduler *Scheduler) RunAfter(duration time.Duration, function task.Function, params ...task.Param) (task.ID, error) {
	return scheduler.RunAt(time.Now().Add(duration), function, params...)
}

// RunEvery will schedule function to be executed every time the duration has elapsed.
func (scheduler *Scheduler) RunEvery(duration time.Duration, function task.Function, persist bool, params ...task.Param) (task.ID, error) {
	funcMeta, err := scheduler.funcRegistry.Add(function)
	if err != nil {
		return "", err
	}

	tsk := task.New(funcMeta, params)

	tsk.IsRecurring = true
	tsk.Duration = duration
	tsk.NextRun = time.Now().Add(duration)

	scheduler.registerTask(tsk, persist)
	return tsk.Hash(), nil
}

// Start will run the scheduler's timer and will trigger the execution
// of tasks depending on their schedule.
// if triggerExpiredTasks is true, out-of-date task will be triggered immediately,
// otherwise they will be deleted
func (scheduler *Scheduler) Start(triggerExpiredTasks bool) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Populate tasks from storage
	if err := scheduler.populateTasks(triggerExpiredTasks); err != nil {
		return err
	}
	if err := scheduler.persistRegisteredTasks(); err != nil {
		return err
	}
	scheduler.runPending()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				scheduler.runPending()
			case <-sigChan:
				scheduler.stopChan <- true
			case <-scheduler.stopChan:
				close(scheduler.stopChan)
			}
		}
	}()

	return nil
}

// Stop will put the scheduler to halt
func (scheduler *Scheduler) Stop() {
	scheduler.stopChan <- true
}

// Wait is a convenience function for blocking until the scheduler is stopped.
func (scheduler *Scheduler) Wait() {
	<-scheduler.stopChan
}

// Cancel is used to cancel the planned execution of a specific task using it's ID.
// The ID is returned when the task was scheduled using RunAt, RunAfter or RunEvery
func (scheduler *Scheduler) Cancel(taskID task.ID) error {
	tsk, found := scheduler.tasks[taskID]
	if !found {
		return fmt.Errorf("Task not found")
	}

	_ = scheduler.taskStore.Remove(tsk)
	delete(scheduler.tasks, taskID)
	return nil
}

// Clear will cancel the execution and clear all registered tasks.
func (scheduler *Scheduler) Clear() {
	for taskID, currentTask := range scheduler.tasks {
		_ = scheduler.taskStore.Remove(currentTask)
		delete(scheduler.tasks, taskID)
	}
	scheduler.funcRegistry = task.NewFuncRegistry()
}

func (scheduler *Scheduler) populateTasks(triggerExpiredTasks bool) error {
	tasks, err := scheduler.taskStore.Fetch()
	if err != nil {
		return err
	}

	for _, dbTask := range tasks {
		//remove taks from db to avoid duplicate
		scheduler.taskStore.Remove(dbTask)

		// If the task instance is still registered with the same computed hash then move on.
		// Otherwise, one of the attributes changed and therefore, the task instance should
		// be added to the list of tasks to be executed with the stored params
		registeredTask, ok := scheduler.tasks[dbTask.Hash()]
		if !ok {
			dbTask.Func, _ = scheduler.funcRegistry.Get(dbTask.Func.Name)
			registeredTask = dbTask
			scheduler.tasks[dbTask.Hash()] = registeredTask
			continue
		}

		// Handle task which is not a recurring one and the NextRun has already passed
		if !dbTask.IsRecurring && dbTask.NextRun.Before(time.Now()) {
			if triggerExpiredTasks {
				(*dbTask).NextRun = time.Now()
				dbTask.Func, _ = scheduler.funcRegistry.Get(dbTask.Func.Name)
				registeredTask = dbTask
				scheduler.tasks[dbTask.Hash()] = registeredTask
			} else {
				_ = scheduler.taskStore.Remove(dbTask)
				delete(scheduler.tasks, dbTask.Hash())
			}
			continue
		}

		// Duration may have changed for recurring tasks
		if dbTask.IsRecurring && registeredTask.Duration != dbTask.Duration {
			// Reschedule NextRun based on dbTask.LastRun + registeredTask.Duration
			registeredTask.NextRun = dbTask.LastRun.Add(registeredTask.Duration)
		}
	}
	return nil
}

func (scheduler *Scheduler) persistRegisteredTasks() error {
	for _, tsk := range scheduler.tasks {
		scheduler.persistTask(tsk)
	}
	return nil
}

func (scheduler *Scheduler) runPending() {
	for _, tsk := range scheduler.tasks {
		if tsk.IsDue() {
			go tsk.Run()

			if !tsk.IsRecurring {
				_ = scheduler.taskStore.Remove(tsk)
				delete(scheduler.tasks, tsk.Hash())
			}
		}
	}
}

func (scheduler *Scheduler) registerTask(task *task.Task, persist bool) {
	_, _ = scheduler.funcRegistry.Add(task.Func)
	scheduler.tasks[task.Hash()] = task
	if persist {
		err := scheduler.persistTask(task)
		if err != nil {
			log.Printf("failed to persist task")
		}
	}
}

func (scheduler *Scheduler) RegisterFuncs(funcs []task.Function) {
	for _, f := range funcs {
		scheduler.funcRegistry.Add(f)
	}
}

func (scheduler *Scheduler) persistTask(task *task.Task) error {
	if task.IsRecurring {
		if task.MustPersist {
			scheduler.taskStore.Remove(task) // prevent duplication
			return scheduler.taskStore.Add(task)
		}
		return nil
	} else {
		return scheduler.taskStore.Add(task)
	}
}
