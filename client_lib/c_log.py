from loguru import logger
import os
import traceback


class Logs:
    def __init__(self, log_dir='logfile/', level='DEBUG', pairs=[], name='default', default_extra_name='default'):
        self.log_dir = log_dir
        self.level = level
        self.name = name
        self.default_extra_name = default_extra_name
        self.pairs = pairs
        self.loggers = {}
        for pair in pairs:
            self.generate_pair_logger(pair)
        self.generate_pair_logger()

    def get_trace(self):
        s = traceback.extract_stack()
        trace_str = str(s[-3])
        trace_str = trace_str.split('/')
        trace_str = trace_str[-1]
        trace_str = trace_str.split(' in ')
        trace_str = trace_str[0].replace(', line ', ':') + ':' + trace_str[1]
        trace_str = trace_str.split(' ')
        return trace_str[len(trace_str) - 1] + ' '

    def generate_pair_logger(self, pair=None):
        if pair is None:
            pair = self.default_extra_name
        log_path = os.path.join(self.log_dir, self.name + "_" + pair + ".log")
        # log_path = os.path.join(self.log_dir, self.name+"_" + pair + "_{time}.log")
        self.loggers[pair] = self.get_logger(log_path, self.level, pair)

    def get_logger(self, path, level, extra_name=None):
        if extra_name is None:
            extra_name = self.default_extra_name
        logger.add(path, format="{time} {level} {message}", level=level,
                   filter=lambda record: record["extra"]["name"] == extra_name, rotation="07:00",
                   encoding="utf-8", enqueue=True, compression="zip", retention="3 days")
        return logger.bind(name=extra_name)

    def info(self, msg, pair=None):
        trace_str = self.get_trace()
        msg = trace_str + str(msg)
        if pair is None:
            self.loggers[self.default_extra_name].info(msg)
            return
        if pair not in self.pairs:
            self.generate_pair_logger(pair)
            self.pairs.append(pair)
        self.loggers[pair].info(msg)

    def warning(self, msg, pair=None):
        trace_str = self.get_trace()
        msg = trace_str + str(msg)
        if pair is None:
            self.loggers[self.default_extra_name].warning(msg)
            return
        if pair not in self.pairs:
            self.generate_pair_logger(pair)
            self.pairs.append(pair)
        self.loggers[pair].warning(msg)

    def debug(self, msg, pair=None):
        trace_str = self.get_trace()
        msg = trace_str + str(msg)
        if pair is None:
            self.loggers[self.default_extra_name].debug(msg)
            return
        if pair not in self.pairs:
            self.generate_pair_logger(pair)
            self.pairs.append(pair)
        self.loggers[pair].debug(msg)

    def error(self, msg, pair=None):
        trace_str = self.get_trace()
        msg = trace_str + str(msg)
        if pair is None:
            self.loggers[self.default_extra_name].error(msg)
            return
        if pair not in self.pairs:
            self.generate_pair_logger(pair)
            self.pairs.append(pair)
        self.loggers[pair].error(msg)

    def write(self, msg, level='info', pair=None):
        trace_str = self.get_trace()
        msg = trace_str + str(msg)
        if pair is None:
            obj = self.loggers[self.default_extra_name]
            getattr(obj, level)(msg)
            return
        if pair not in self.pairs:
            self.generate_pair_logger(pair)
            self.pairs.append(pair)
        obj = self.loggers[pair]
        getattr(obj, level)(msg)
