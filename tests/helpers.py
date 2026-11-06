"""Shared mock fixtures and test helpers for reconciliation tests."""
from __future__ import annotations


class MockCursor:
    def __init__(self, query_responses):
        self.query_responses = query_responses
        self.call_index = 0
        self.current_rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        if self.call_index < len(self.query_responses):
            self.current_rows = self.query_responses[self.call_index]
            self.call_index += 1
        else:
            self.current_rows = []

    def fetchall(self):
        return self.current_rows


class MockConnection:
    def __init__(self, query_responses):
        self.query_responses = query_responses

    def cursor(self):
        return MockCursor(self.query_responses)
