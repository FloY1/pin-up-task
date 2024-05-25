import subprocess
import time

from watchdog.events import FileSystemEventHandler, FileSystemEvent, EVENT_TYPE_CREATED, EVENT_TYPE_DELETED
from watchdog.observers import Observer


class EventHandler(FileSystemEventHandler):
    SECOND_SCRIPT_NAME = "script2.py"

    def on_any_event(self, event: FileSystemEvent):
        if event.is_directory:
            return None
        elif event.event_type in (EVENT_TYPE_CREATED, EVENT_TYPE_DELETED):
            subprocess.run(["python", self.SECOND_SCRIPT_NAME])


class Watcher:
    DIRECTORY_TO_WATCH_PAYMENTS = "payments"
    DIRECTORY_TO_WATCH_BETS = "bets"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = EventHandler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH_PAYMENTS, recursive=False)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH_BETS, recursive=False)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()


if __name__ == '__main__':
    w = Watcher()
    w.run()
