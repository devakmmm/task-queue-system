# task_queue_system.py
import asyncio
import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from queue import PriorityQueue, Empty
from threading import Lock, Thread
from typing import Dict, Any, Optional, Callable, List
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class TaskPriority(Enum):
    LOW = 3
    NORMAL = 2
    HIGH = 1
    URGENT = 0


@dataclass
class Task:
    id: str
    func_name: str
    args: tuple
    kwargs: dict
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = 3
    retry_count: int = 0
    created_at: datetime = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error_message: Optional[str] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.id is None:
            self.id = str(uuid.uuid4())

    def __lt__(self, other):
        # For priority queue ordering
        return self.priority.value < other.priority.value

    def to_dict(self):
        data = asdict(self)
        data['created_at'] = self.created_at.isoformat() if self.created_at else None
        data['started_at'] = self.started_at.isoformat() if self.started_at else None
        data['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        data['priority'] = self.priority.name
        data['status'] = self.status.value
        return data


class TaskRegistry:
    """Registry for available task functions"""
    
    def __init__(self):
        self._tasks: Dict[str, Callable] = {}
        self._lock = Lock()

    def register(self, name: str, func: Callable):
        with self._lock:
            self._tasks[name] = func
            logger.info(f"Registered task function: {name}")

    def get(self, name: str) -> Optional[Callable]:
        with self._lock:
            return self._tasks.get(name)

    def list_tasks(self) -> List[str]:
        with self._lock:
            return list(self._tasks.keys())


class TaskQueue:
    """Thread-safe priority task queue"""
    
    def __init__(self, maxsize: int = 0):
        self._queue = PriorityQueue(maxsize)
        self._task_storage: Dict[str, Task] = {}
        self._lock = Lock()

    def put(self, task: Task, block: bool = True, timeout: Optional[float] = None):
        """Add a task to the queue"""
        with self._lock:
            self._task_storage[task.id] = task
        
        self._queue.put(task, block, timeout)
        logger.info(f"Task {task.id} added to queue with priority {task.priority.name}")

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Task:
        """Get a task from the queue"""
        task = self._queue.get(block, timeout)
        return task

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task status by ID"""
        with self._lock:
            task = self._task_storage.get(task_id)
            return task.to_dict() if task else None

    def update_task(self, task: Task):
        """Update task in storage"""
        with self._lock:
            self._task_storage[task.id] = task

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks with their status"""
        with self._lock:
            return [task.to_dict() for task in self._task_storage.values()]

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()


class Worker:
    """Worker that processes tasks from the queue"""
    
    def __init__(self, worker_id: str, task_queue: TaskQueue, task_registry: TaskRegistry):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.task_registry = task_registry
        self.is_running = False
        self._thread: Optional[Thread] = None
        self.processed_count = 0
        self.failed_count = 0

    def start(self):
        """Start the worker in a separate thread"""
        if not self.is_running:
            self.is_running = True
            self._thread = Thread(target=self._work_loop, daemon=True)
            self._thread.start()
            logger.info(f"Worker {self.worker_id} started")

    def stop(self):
        """Stop the worker"""
        self.is_running = False
        if self._thread:
            self._thread.join()
        logger.info(f"Worker {self.worker_id} stopped")

    def _work_loop(self):
        """Main worker loop"""
        while self.is_running:
            try:
                # Get task with timeout to allow checking stop condition
                task = self.task_queue.get(block=True, timeout=1.0)
                self._process_task(task)
            except Empty:
                continue  # Timeout, check if we should stop
            except Exception as e:
                logger.error(f"Worker {self.worker_id} encountered error: {e}")

    def _process_task(self, task: Task):
        """Process a single task"""
        logger.info(f"Worker {self.worker_id} processing task {task.id}")
        
        # Update task status
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        self.task_queue.update_task(task)

        try:
            # Get the function to execute
            func = self.task_registry.get(task.func_name)
            if func is None:
                raise ValueError(f"Task function '{task.func_name}' not found")

            # Execute the task
            result = func(*task.args, **task.kwargs)
            
            # Task completed successfully
            task.status = TaskStatus.COMPLETED
            task.result = result
            task.completed_at = datetime.now()
            self.processed_count += 1
            
            logger.info(f"Worker {self.worker_id} completed task {task.id}")

        except Exception as e:
            # Task failed
            task.error_message = str(e)
            
            if task.retry_count < task.max_retries:
                # Retry the task
                task.retry_count += 1
                task.status = TaskStatus.RETRYING
                logger.warning(f"Task {task.id} failed, retrying ({task.retry_count}/{task.max_retries})")
                
                # Add delay before retry (exponential backoff)
                retry_delay = 2 ** task.retry_count
                time.sleep(retry_delay)
                
                # Reset status and re-queue
                task.status = TaskStatus.PENDING
                task.started_at = None
                self.task_queue.put(task)
            else:
                # Max retries reached
                task.status = TaskStatus.FAILED
                task.completed_at = datetime.now()
                self.failed_count += 1
                logger.error(f"Task {task.id} failed permanently after {task.max_retries} retries: {e}")

        finally:
            # Always update task status
            self.task_queue.update_task(task)

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics"""
        return {
            'worker_id': self.worker_id,
            'is_running': self.is_running,
            'processed_count': self.processed_count,
            'failed_count': self.failed_count
        }


class TaskManager:
    """Central manager for the task queue system"""
    
    def __init__(self, max_workers: int = 4, queue_size: int = 100):
        self.task_queue = TaskQueue(queue_size)
        self.task_registry = TaskRegistry()
        self.workers: List[Worker] = []
        self.max_workers = max_workers
        self._is_running = False

    def register_task(self, name: str, func: Callable):
        """Register a task function"""
        self.task_registry.register(name, func)

    def submit_task(
        self,
        func_name: str,
        *pos_args,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
        **kw,
    ) -> str:
        """
        Submit a new task.

        Accepts either:
          - Legacy positional: submit_task("add", 10, 20, priority=...)
          - Named args:        submit_task(func_name="add", args=[10,20], kwargs={"x": 1}, ...)

        Any 'args'/'kwargs' passed by name are merged with positional ones.
        """
        # Extract named args/kwargs if supplied by API layer
        args_from_kw = kw.pop("args", None)       # e.g., [10, 20]
        kwargs_from_kw = kw.pop("kwargs", None)   # e.g., {"x": 1}

        # Merge positional args with named args
        if args_from_kw is not None:
            if isinstance(args_from_kw, (list, tuple)):
                merged_args = tuple(args_from_kw) + tuple(pos_args)
            else:
                raise TypeError("'args' must be a list or tuple")
        else:
            merged_args = tuple(pos_args)

        # Final kwargs for the callable
        if kwargs_from_kw is not None:
            if not isinstance(kwargs_from_kw, dict):
                raise TypeError("'kwargs' must be a dict")
            call_kwargs = dict(kwargs_from_kw)
        else:
            call_kwargs = {}

        # (Optional) kw may contain unexpected keys; ignore or log:
        # if kw: logger.debug(f"Ignoring extra submit_task kwargs: {kw}")

        task = Task(
            id=str(uuid.uuid4()),
            func_name=func_name,
            args=merged_args,
            kwargs=call_kwargs,
            priority=priority,
            max_retries=max_retries
        )
        
        self.task_queue.put(task)
        return task.id

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific task"""
        return self.task_queue.get_task_status(task_id)

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks"""
        return self.task_queue.get_all_tasks()

    def start(self):
        """Start the task manager and workers"""
        if self._is_running:
            return
        
        self._is_running = True
        
        # Create and start workers
        for i in range(self.max_workers):
            worker = Worker(f"worker-{i}", self.task_queue, self.task_registry)
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"Task manager started with {self.max_workers} workers")

    def stop(self):
        """Stop the task manager and all workers"""
        if not self._is_running:
            return
        
        self._is_running = False
        
        # Stop all workers
        for worker in self.workers:
            worker.stop()
        
        self.workers.clear()
        logger.info("Task manager stopped")

    def get_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        worker_stats = [worker.get_stats() for worker in self.workers]
        
        return {
            'queue_size': self.task_queue.qsize(),
            'total_tasks': len(self.task_queue.get_all_tasks()),
            'available_task_types': self.task_registry.list_tasks(),
            'workers': worker_stats,
            'is_running': self._is_running
        }


# Example task functions
def add_numbers(a: int, b: int) -> int:
    """Example task: add two numbers"""
    time.sleep(1)  # Simulate some work
    return a + b

def multiply_numbers(a: int, b: int) -> int:
    """Example task: multiply two numbers"""
    time.sleep(2)  # Simulate some work
    return a * b


def simulate_work(duration: int, fail: bool = False) -> str:
    """Example task: simulate work that might fail"""
    time.sleep(duration)
    if fail:
        raise Exception("Simulated failure")
    return f"Work completed after {duration} seconds"


def process_data(data: List[int]) -> Dict[str, Any]:
    """Example task: process a list of numbers"""
    time.sleep(1)
    return {
        'sum': sum(data),
        'avg': sum(data) / len(data) if data else 0,
        'count': len(data),
        'max': max(data) if data else 0,
        'min': min(data) if data else 0
    }


# Demo usage
if __name__ == "__main__":
    # Create task manager
    manager = TaskManager(max_workers=3)
    
    # Register task functions
    manager.register_task("add", add_numbers)
    manager.register_task("multiply", multiply_numbers)
    manager.register_task("simulate_work", simulate_work)
    manager.register_task("process_data", process_data)
    
    # Start the system
    manager.start()
    
    try:
        # Submit various tasks
        task_ids = []
        
        # High priority task (positional args)
        task_id = manager.submit_task("add", 10, 20, priority=TaskPriority.HIGH)
        task_ids.append(task_id)
        print(f"Submitted high priority add task: {task_id}")
        
        # Normal priority tasks
        task_id = manager.submit_task("multiply", 5, 6)
        task_ids.append(task_id)
        print(f"Submitted multiply task: {task_id}")
        
        # Task with data processing (positional arg list)
        task_id = manager.submit_task("process_data", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        task_ids.append(task_id)
        print(f"Submitted data processing task: {task_id}")
        
        # Task that will fail and retry (kwargs)
        task_id = manager.submit_task("simulate_work", 1, priority=TaskPriority.NORMAL, max_retries=2, kwargs={"fail": True})
        task_ids.append(task_id)
        print(f"Submitted failing task: {task_id}")
        
        # Task that will succeed (kwargs)
        task_id = manager.submit_task("simulate_work", args=[2], kwargs={"fail": False})
        task_ids.append(task_id)
        print(f"Submitted successful work task: {task_id}")
        
        # Monitor progress
        print("\n--- Monitoring Task Progress ---")
        completed_tasks = set()
        
        while len(completed_tasks) < len(task_ids):
            time.sleep(2)
            
            for task_id in task_ids:
                if task_id not in completed_tasks:
                    status = manager.get_task_status(task_id)
                    if status:
                        print(f"Task {task_id[:8]}... - Status: {status['status']}")
                        
                        if status['status'] in ['completed', 'failed']:
                            completed_tasks.add(task_id)
                            if status['status'] == 'completed':
                                print(f"  Result: {status['result']}")
                            else:
                                print(f"  Error: {status['error_message']}")
        
        # Show final stats
        print("\n--- Final Statistics ---")
        stats = manager.get_stats()
        print(json.dumps(stats, indent=2, default=str))
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        manager.stop()
        print("System stopped")
