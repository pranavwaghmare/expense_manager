import os

RUN_ENV = os.environ.get("RUN_ENV", "local")
LOCAL_HOSTS = {
    "dfs-meta": "localhost:60501",
    "dfs-store-1": "localhost:60511",
    "dfs-store-2": "localhost:60512",
    "dfs-store-3": "localhost:60513",
}
DOCKER_HOSTS = {
    name: address.replace("localhost", name)
    for name, address in LOCAL_HOSTS.items()
}


def resolve(name: str) -> str:
    hosts = DOCKER_HOSTS if RUN_ENV == "docker" else LOCAL_HOSTS
    try:
        return hosts[name]
    except KeyError as exc:
        raise ValueError(f"unknown DFS service: {name}") from exc
