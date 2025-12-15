import re

def make_safe_prefix(url: str, max_len: int = 80) -> str:
    """
    Convert an arbitrary URL into a filename-safe prefix.
    - Replace non-alphanumeric characters with '_'
    - Collapse multiple '_' into one
    - Trim to max_len
    - Ensure it ends with '_' (if not empty)
    """
    # Replace non-alnum with '_'
    prefix = re.sub(r"[^A-Za-z0-9]+", "_", url)

    # Collapse multiple underscores
    prefix = re.sub(r"_+", "_", prefix).strip("_")

    if not prefix:
        return ""

    # Trim
    if len(prefix) > max_len:
        prefix = prefix[:max_len].rstrip("_")

    # Ensure trailing underscore to separate from basename
    return prefix + "_"