# simple_task_queue.py
import time
import threading
import uuid
from queue import PriorityQueue, Empty
from datetime import datetime
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional, Callable, List

# Configure basic logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

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
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error_message: Optional[str] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.id is None:
            self.id = str(uuid.uuid4())

    def __lt__(self, other):
        return self.priority.value < other.priority.value

class TaskRegistry:
    def __init__(self):
        self._tasks: Dict[str, Callable] = {}
        self._lock = threading.Lock()

    def register(self, name: str, func: Callable):
        with self._lock:
            self._tasks[name] = func
            logger.info(f"Registered task function: {name}")

    def get(self, name: str) -> Optional[Callable]:
        with self._lock:
            return self._tasks.get(name)

class TaskQueue:
    def __init__(self):
        self._queue = PriorityQueue()
        self._task_storage: Dict[str, Task] = {}
        self._lock = threading.Lock()

    def put(self, task: Task):
        with self._lock:
            self._task_storage[task.id] = task
        self._queue.put(task)
        logger.info(f"Task {task.id} added to queue")

    def get(self, timeout: Optional[float] = None) -> Task:
        return self._queue.get(timeout=timeout)

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            task = self._task_storage.get(task_id)
            if task:
                return {
                    'id': task.id,
                    'func_name': task.func_name,
                    'status': task.status.value,
                    'result': task.result,
                    'error_message': task.error_message,
                    'created_at': task.created_at.isoformat(),
                    'priority': task.priority.name
                }
            return None

    def update_task(self, task: Task):
        with self._lock:
            self._task_storage[task.id] = task

class Worker:
    def __init__(self, worker_id: str, task_queue: TaskQueue, task_registry: TaskRegistry):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.task_registry = task_registry
        self.is_running = False
        self._thread: Optional[threading.Thread] = None
        self.processed_count = 0

    def start(self):
        if not self.is_running:
            self.is_running = True
            self._thread = threading.Thread(target=self._work_loop, daemon=True)
            self._thread.start()
            logger.info(f"Worker {self.worker_id} started")

    def stop(self):
        self.is_running = False
        if self._thread:
            self._thread.join()
        logger.info(f"Worker {self.worker_id} stopped")

    def _work_loop(self):
        while self.is_running:
            try:
                task = self.task_queue.get(timeout=1.0)
                self._process_task(task)
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")

    def _process_task(self, task: Task):
        logger.info(f"Worker {self.worker_id} processing task {task.id}")
        
        task.status = TaskStatus.RUNNING
        self.task_queue.update_task(task)

        try:
            func = self.task_registry.get(task.func_name)
            if func is None:
                raise ValueError(f"Task function '{task.func_name}' not found")

            result = func(*task.args, **task.kwargs)
            task.status = TaskStatus.COMPLETED
            task.result = result
            self.processed_count += 1
            logger.info(f"Worker {self.worker_id} completed task {task.id}")

        except Exception as e:
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.PENDING
                logger.warning(f"Task {task.id} failed, retrying ({task.retry_count}/{task.max_retries})")
                time.sleep(2 ** task.retry_count)  # Exponential backoff
                self.task_queue.put(task)
            else:
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
                logger.error(f"Task {task.id} failed permanently: {e}")

        finally:
            self.task_queue.update_task(task)

class TaskManager:
    def __init__(self, max_workers: int = 2):
        self.task_queue = TaskQueue()
        self.task_registry = TaskRegistry()
        self.workers: List[Worker] = []
        self.max_workers = max_workers
        self._is_running = False

    def register_task(self, name: str, func: Callable):
        self.task_registry.register(name, func)

    def submit_task(self, func_name: str, *args, priority: TaskPriority = TaskPriority.NORMAL, 
                   max_retries: int = 3, **kwargs) -> str:
        task = Task(
            id=str(uuid.uuid4()),
            func_name=func_name,
            args=args,
            kwargs=kwargs,
            priority=priority,
            max_retries=max_retries
        )
        
        self.task_queue.put(task)
        return task.id

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        return self.task_queue.get_task_status(task_id)

    def start(self):
        if self._is_running:
            return
        
        self._is_running = True
        for i in range(self.max_workers):
            worker = Worker(f"worker-{i}", self.task_queue, self.task_registry)
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"Task manager started with {self.max_workers} workers")

    def stop(self):
        if not self._is_running:
            return
        
        self._is_running = False
        for worker in self.workers:
            worker.stop()
        self.workers.clear()
        logger.info("Task manager stopped")

# Example task functions
def add_numbers(a: int, b: int) -> int:
    """Add two numbers"""
    time.sleep(1)  # Simulate work
    return a + b

def multiply_numbers(a: int, b: int) -> int:
    """Multiply two numbers"""
    time.sleep(1)  # Simulate work
    return a * b

def process_text(text: str) -> dict:
    """Process text and return stats"""
    time.sleep(0.5)  # Simulate work
    return {
        'length': len(text),
        'words': len(text.split()),
        'upper': text.upper()
    }

def simulate_work(duration: int, should_fail: bool = False) -> str:
    """Simulate work that might fail"""
    time.sleep(duration)
    if should_fail:
        raise Exception("Simulated failure")
    return f"Work completed after {duration} seconds"

# Demo
if __name__ == "__main__":
    print("🚀 Starting Simple Task Queue System")
    
    # Create and configure manager
    manager = TaskManager(max_workers=2)
    
    # Register task functions
    manager.register_task("add", add_numbers)
    manager.register_task("multiply", multiply_numbers)
    manager.register_task("process_text", process_text)
    manager.register_task("simulate_work", simulate_work)
    
    # Start the system
    manager.start()
    
    try:
        print("\n📝 Submitting tasks...")
        
        # Submit various tasks
        task1 = manager.submit_task("add", 10, 20)
        print(f"✅ Submitted add task: {task1[:8]}...")
        
        task2 = manager.submit_task("multiply", 5, 6, priority=TaskPriority.HIGH)
        print(f"✅ Submitted high-priority multiply task: {task2[:8]}...")
        
        task3 = manager.submit_task("process_text", "Hello World from Task Queue!")
        print(f"✅ Submitted text processing task: {task3[:8]}...")
        
        task4 = manager.submit_task("simulate_work", 2, should_fail=False)
        print(f"✅ Submitted work simulation task: {task4[:8]}...")
        
        # Task that will fail and retry
        task5 = manager.submit_task("simulate_work", 1, should_fail=True, max_retries=2)
        print(f"✅ Submitted failing task: {task5[:8]}...")
        
        print(f"\n⏳ Processing {len([task1, task2, task3, task4, task5])} tasks...")
        
        # Monitor progress
        all_task_ids = [task1, task2, task3, task4, task5]
        completed_tasks = set()
        
        while len(completed_tasks) < len(all_task_ids):
            time.sleep(1)
            
            for task_id in all_task_ids:
                if task_id not in completed_tasks:
                    status = manager.get_task_status(task_id)
                    if status and status['status'] in ['completed', 'failed']:
                        completed_tasks.add(task_id)
                        
                        if status['status'] == 'completed':
                            print(f"✅ Task {task_id[:8]}... completed: {status['result']}")
                        else:
                            print(f"❌ Task {task_id[:8]}... failed: {status['error_message']}")
                    elif status:
                        print(f"⏳ Task {task_id[:8]}... status: {status['status']}")
        
        print(f"\n🎉 All tasks completed!")
        
        # Show summary
        print("\n📊 Final Results:")
        for i, task_id in enumerate(all_task_ids, 1):
            status = manager.get_task_status(task_id)
            result_str = str(status['result']) if status['result'] else status['error_message']
            print(f"Task {i}: {status['status'].upper()} - {result_str}")
        
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        manager.stop()
        print("👋 System stopped cleanly")