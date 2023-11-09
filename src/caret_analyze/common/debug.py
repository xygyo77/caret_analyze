import sys, os
import time

### for debug
g_start_time = time.time()
import inspect
class loc:
    @staticmethod
    def loc(depth=0):
        """Get execute loaction (file name & line number)

        Args:
            depth (int, optional): _description_. Defaults to 0.

        Returns:
            _type_: file name & line number
        """
        elapse = time.time() - g_start_time
        frame = inspect.currentframe().f_back
        return print(f"[{elapse}] {os.path.basename(frame.f_code.co_filename), frame.f_code.co_name, frame.f_lineno}")
