"""Settings that define what Dorado measures."""

# Repositories that merge on GitHub and label their bugs. See the README for why
# each one is on the list and why some well-known projects are not.
REPOSITORIES: tuple[str, ...] = (
    "kubernetes/kubernetes",
    "grafana/grafana",
    "elastic/elasticsearch",
    "microsoft/vscode",
    "apache/airflow",
    "dotnet/runtime",
    "astral-sh/uv",
    "astral-sh/ruff",
)
