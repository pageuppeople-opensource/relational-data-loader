import importlib
import logging


def create_type_instance(type_name):
    module = importlib.import_module(type_name)
    class_ = getattr(module, type_name)
    instance = class_()
    return instance


class SensitiveDataError(BaseException):
    def add_sensitive_error_args(self, sensitive_error_args):
        self.sensitive_error_args = sensitive_error_args
        return self


def prevent_senstive_data_logging(function):
    # https://stackoverflow.com/questions/2052390/manually-raising-throwing-an-exception-in-python

    def wrapper(self, *args, **kwargs):
        logger = self.logger or logging.getLogger(__name__)
        if logger.getEffectiveLevel() == logging.DEBUG:
            return function(self, *args, **kwargs)

        try:
            return function(self, *args, **kwargs)
        except Exception as e:
            err_str = f"A {e.__class__} occured in {function.__name__}. Re-run with --log-level DEBUG to override"
            raise SensitiveDataError(err_str).with_traceback(e.__traceback__).add_sensitive_error_args(e.args) from None

    return wrapper
