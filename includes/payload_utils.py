from pydantic import BaseModel


class Payload(BaseModel):
    message: str
    env: str
    source: str
    numbers: list[int]


def build_payload(message: str, env: str, source: str, numbers: list[int]) -> Payload:
    return Payload(
        message=message,
        env=env,
        source=source,
        numbers=numbers,
    )
