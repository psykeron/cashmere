from src.exception import one_line_error


class TestOneLineError:
    def test_one_line_error(self):
        exception = Exception("well hello")
        try:
            raise exception
        except Exception as e:
            error_str = one_line_error(e)
            assert error_str.startswith("Traceback (most recent call last):\\n\\n")
            assert len(error_str.splitlines()) == 1
