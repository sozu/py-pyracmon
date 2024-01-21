from collections.abc import Callable
from contextvars import ContextVar
from datetime import datetime, date, time, timedelta
from typing import Any, Optional
from ..config import PyracmonConfiguration, default_config, contextualConfiguration


config: ContextVar[PyracmonConfiguration] = ContextVar('config', default=contextualConfiguration(lambda: config, default_config()))


def default_test_config() -> PyracmonConfiguration:
    return config.get()


class Matcher:
    def __init__(self):
        self.invert = False

    def __invert__(self):
        self.invert = True
        return self

    def __and__(self, another):
        return CompositeMatcher(self, another, True)

    def __or__(self, another):
        return CompositeMatcher(self, another, False)

    def match(self, actual):
        return self._match(actual) ^ self.invert

    def _match(self, actual):
        raise NotImplementedError()


class CompositeMatcher(Matcher):
    def __init__(self, m1, m2, and_=True):
        super().__init__()
        self.m1 = m1
        self.m2 = m2
        self.and_ = and_

    def _match(self, actual):
        if self.and_:
            return self.m1.match(actual) and self.m2.match(actual)
        else:
            return self.m1.match(actual) or self.m2.match(actual)


class Near(Matcher):
    def __init__(self, expected, negative=None, positive=None, **kwargs):
        super().__init__()
        self.expected = expected
        self.negative = negative
        self.positive = positive
        self.kwargs = kwargs

    def _margin(self, t):
        neg = self.negative or 0
        pos = self.positive or 0

        if issubclass(t, (int, float)):
            return (self.expected + neg, self.expected + pos)
        elif issubclass(t, (datetime, date)):
            delta_args = {k:v for k, v in self.kwargs.items() if k in {
                'weeks', 'days', 'hours', 'minutes', 'seconds', 'milliseconds', 'microseconds',
            }}
            delta = timedelta(**delta_args)
            return (self.expected + neg * delta, self.expected + pos * delta)
        else:
            raise ValueError(f"Margin for {t} is not supported.")

    def _match(self, actual):
        if self.negative is None and self.positive is None:
            return actual == self.expected
        else:
            low, high = self._margin(type(actual))
            return low <= actual and actual <= high


def near(expected: Any, negative: Optional[Any] = None, positive: Optional[Any] = None, **kwargs: Any) -> Matcher:
    """
    Creates a matcher to check actual value is in a range.

    Comparison between the value and given range depends on its type.

    Args:
        expected: Expected value.
        negative: Margin of negative direction.
        positive: Margin of positive direction.
        kwargs: Keyword arguments to create marginal values.
    Returns:
        Created matcher.
    """
    if isinstance(expected, datetime):
        if all(k not in kwargs for k in ('weeks', 'days', 'hours', 'minutes', 'seconds', 'milliseconds', 'microseconds')):
            kwargs.update(**default_test_config().timedelta_unit)
    return Near(expected, negative, positive, **kwargs)


class let(Matcher):
    def __init__(self, pred: Callable[[Any], bool]) -> None:
        """
        Creates a matcher which applies the predicate function to actual value and checks its returning value is ``True`` .

        Args:
            pred: A predicate function.
        Returns:
            Created matcher.
        """
        super().__init__()
        self.pred = pred

    def _match(self, actual):
        return self.pred(actual)


class one_of(Matcher):
    def __init__(self, *candidates) -> None:
        """
        Creates a matcher which checks whether the actual value matches one of candidate values.

        Args:
            candidates: Candidate values.
        Returns:
            Created matcher.
        """
        super().__init__()
        self.candidates = candidates

    def _match(self, actual):
        for c in self.candidates:
            if isinstance(c, Matcher):
                if c.match(actual):
                    return True
            elif c == actual:
                return True
        return False