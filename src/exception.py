import traceback


def one_line_error(exception: Exception) -> str:
    return "\\n".join(
        line.replace("\n", "\\n") for line in traceback.format_exception(exception)
    )


class CashmereException(Exception): ...
