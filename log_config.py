import logging


class ScriptLogger:
    """
    Log-levels:
    - 50 = CRITICAL
    - 40 = ERROR
    - 30 = WARNING
    - 20 = INFO
    - 10 = DEBUG
    """
    def __init__(
        self,
        target_file,
        *,
        level=20,
        format='%(asctime)s [%(levelname)s] %(message)s',
        filemode='w'
    ):
        self.log = logging.getLogger(target_file)
        self.log.setLevel(level)

        if not self.log.handlers:  # Prevent adding handlers multiple times
            handler = logging.FileHandler(target_file, mode=filemode)
            formatter = logging.Formatter(format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
