from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    '''
    this is an important process, which is why it is __init__.py 
    this will create the frontier and worker objects
        based on the thread count, that is the amount of workers that are created
    '''
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

    # starts the workers in different threads
    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    # combines the start_async() and join() to run the charlwer synchronously
    def start(self):
        self.start_async()
        self.join()

    # waits for all workers to finish
    def join(self):
        for worker in self.workers:
            worker.join()
