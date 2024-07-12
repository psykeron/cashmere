import re


def translate_event_name_to_sns_topic_name(name: str, is_fifo: bool = False) -> str:
    if not name:
        raise ValueError(
            f"Topic name {name} is too short. It must be 1 to 80 characters."
        )

    # replace all non-alphanumeric characters (except - and _) with underscores
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)

    if is_fifo:
        name = f"{name}.fifo"

    if len(name) > 80:
        raise ValueError(
            f"Topic name {name} is too long. It must be 1 to 80 characters."
        )

    return name


def translate_queue_name_to_sqs_queue_name(
    name: str, is_fifo: bool = False, is_deadletter: bool = False
) -> str:
    if not name:
        raise ValueError(
            f"Queue name {name} is too short. It must be 1 to 80 characters."
        )

    # replace all non-alphanumeric characters (except - and _) with underscores
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)

    if is_deadletter:
        name = f"{name}-error"

    if is_fifo:
        name = f"{name}.fifo"

    if len(name) > 80:
        raise ValueError(
            f"Queue name {name} is too long. It must be 1 to 80 characters."
        )

    return name
