import logging
import inspect


class CustomFormatter(logging.Formatter):

    def format(self, record):
        # Inject function name and line number into the record
        stack = inspect.stack()
        record.funcName = stack[8].function if len(stack) > 8 else ''
        record.lineno = stack[8].lineno if len(stack) > 8 else ''
        return super().format(record)


class Logger(logging.Logger):
    def __init__(self, name=None, level=logging.INFO, log_file=None):
        """
        Initialize the custom logger.

        :param name: The name of the logger. If None, the root logger is used.
        :param level: The logging level. Defaults to logging.INFO.
        :param log_file: The file to log to. If None, logs to stdout.
        """
        super().__init__(name, level)
        if not self.hasHandlers():
            # Create a console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                CustomFormatter('%(asctime)s - %(levelname)s - %(funcName)s(%(lineno)d) - %(message)s'))
            self.addHandler(console_handler)

        if log_file:
            # Create a file handler
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(
                CustomFormatter('%(asctime)s - %(levelname)s - %(funcName)s(%(lineno)d) - %(message)s'))
            self.addHandler(file_handler)

    def setDebugLevel(self):
        self.setLevel(logging.DEBUG)

    def setInfoLevel(self):
        self.setLevel(logging.INFO)

    def setWarningLevel(self):
        self.setLevel(logging.WARNING)

    def setErrorLevel(self):
        self.setLevel(logging.ERROR)

    def setCriticalLevel(self):
        self.setLevel(logging.CRITICAL)

    def setLogLevel(self, level):
        self.setLevel(level)
