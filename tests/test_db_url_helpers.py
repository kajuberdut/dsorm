import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dsorm.db_url_helpers import get_absolute_path, normalize_database_url


class TestNormalizeDatabaseURL(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("dsorm.db_url_helpers.Path")
        self.mock_path = self.patcher.start()

        self.cwd = "/current/working/directory"
        self.user_home = "/home/user"

    def tearDown(self):
        self.patcher.stop()

    def test_normalize_database_url_with_url(self):
        examples = [
            "sqlite+aiosqlite:///temp.db",
            "postgresql+asyncpg://localhost/example",
        ]

        for url in examples:
            self.assertEqual(normalize_database_url(url), url)

    def test_get_absolute_path(self):
        examples = [
            ("temp.db", f"{self.cwd}/temp.db"),
            ("~/temp.db", f"{self.user_home}/temp.db"),
        ]
        for filepath, expected_path in examples:
            path_instance = MagicMock()
            path_instance.expanduser.return_value = Path(expected_path)
            self.mock_path.return_value = path_instance
            self.mock_path.cwd.return_value = Path(self.cwd)

            self.assertEqual(
                str(get_absolute_path(filepath)),
                str(Path(expected_path)),
            )

    def test_normalize_database_url_with_filepath(self):
        examples = [
            ("temp.db", f"sqlite+aiosqlite://{self.cwd}/temp.db"),
            ("~/temp.db", f"sqlite+aiosqlite://{self.user_home}/temp.db"),
        ]

        for filepath, expected_url in examples:
            path_instance = MagicMock()
            path_instance.expanduser.return_value = Path(
                expected_url.split("://", 1)[1]
            )
            self.mock_path.return_value = path_instance
            cwd_mock = MagicMock()
            cwd_mock.__truediv__.return_value = Path(expected_url.split("://", 1)[1])
            self.mock_path.cwd.return_value = cwd_mock

            self.assertEqual(normalize_database_url(filepath), expected_url)


if __name__ == "__main__":
    unittest.main()
